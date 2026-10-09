package controller

// Canary rollout of aggregator config (docs/adr/0002). Each config revision
// is a file vector-<hash>.yaml in the aggregator's ConfigMap; a pod loads the
// one named by its template's config-suffix annotation. A new revision goes
// to the highest-ordinal pod first (the StatefulSet's partition), bakes while
// its errors are compared with a stable pod's, and is then promoted to every
// pod or rolled back. The state lives in annotations on the StatefulSet, so a
// restarted operator picks up where it left off.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// BootstrapKey is the config a pod loads before the operator has picked
	// one for it (no suffix annotation): the render with no pipelines.
	BootstrapKey = "vector.yaml"
	// SuffixAnnotation on the pod template selects the pod's config file:
	// vector<suffix>.yaml, with suffix "-<hash>".
	SuffixAnnotation = "spillway.dev/config-suffix"
	// SuffixEnv is the environment variable the aggregator's args use to
	// expand the suffix, filled from SuffixAnnotation by the downward API.
	SuffixEnv = "SPILLWAY_CONFIG_SUFFIX"

	annStable          = "spillway.dev/stable-config"
	annStablePipelines = "spillway.dev/stable-pipelines"
	annCanary          = "spillway.dev/canary-config"
	annCanaryStarted   = "spillway.dev/canary-started"
	annCanaryBaseline  = "spillway.dev/canary-baseline"
	annCanaryPipelines = "spillway.dev/canary-pipelines"
	annRejected        = "spillway.dev/rejected-config"
	annRejectedReason  = "spillway.dev/rejected-reason"
	// annQuarantine maps pipeline UID to the generation that failed a canary
	// (and why). Later configs leave that generation out until the spec
	// changes, so one bad change doesn't ride along with everyone else's.
	annQuarantine = "spillway.dev/quarantined-pipelines"

	// LastGoodSpecAnnotation on a LogPipeline holds the spec (and generation)
	// last promoted to every aggregator, so a pipeline whose newer spec is
	// quarantined keeps running the version that worked.
	LastGoodSpecAnnotation = "spillway.dev/last-good-spec"
)

// quarantined is one pipeline generation that failed a canary.
type quarantined struct {
	Generation int64  `json:"generation"`
	Reason     string `json:"reason"`
	// Retry marks a change that failed in a canary together with others, so
	// it's not known to be at fault: it's retried in a canary of its own.
	Retry bool `json:"retry,omitempty"`
}

// configKey is the ConfigMap key for a config revision.
func configKey(hash string) string { return "vector-" + hash + ".yaml" }

// CanarySettings tune the canary.
type CanarySettings struct {
	// Bake is how long the canary runs, once Ready, before it's judged.
	Bake time.Duration
	// ReadyTimeout is how long the canary pod may take to become Ready.
	ReadyTimeout time.Duration
	// The canary may log more errors than the stable pod during the bake,
	// up to the larger of ErrorAllowance and ErrorRatio times the events its
	// sinks took in, so the allowance scales with traffic.
	ErrorAllowance float64
	ErrorRatio     float64
}

var (
	rollouts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "spillway_operator_rollouts_total",
		Help: "Aggregator config canaries, by result (promoted or rolled_back).",
	}, []string{"result"})
	orphanedBuffers = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "spillway_operator_orphaned_buffers_total",
		Help: "Rollbacks that left a sink's disk buffer on the canary pod with nothing to read it (issue #43).",
	}, []string{"sink"})
)

func init() { metrics.Registry.MustRegister(rollouts, orphanedBuffers) }

type phase int

const (
	phaseStable   phase = iota // every pod runs the desired config
	phaseRolling               // the desired config is rolling to every pod
	phaseCanary                // the desired config runs on the canary only
	phaseRejected              // the desired config failed its canary
)

// rolloutState is what the statuses are built from.
type rolloutState struct {
	phase   phase
	message string
	// stable maps pipeline UID to the generation running in the stable config.
	stable map[string]int64
}

type baseline struct {
	At time.Time `json:"at"`
	// Bake is the bake this canary gets, fixed when it starts.
	Bake   time.Duration `json:"bake"`
	Canary PodHealth     `json:"canary"`
	Stable PodHealth     `json:"stable"`
}

// bakeFor returns how long to bake cfg: the configured bake, but at least
// twice the longest sink batch timeout, so a sink that batches gets to send
// at least once before a stall can be judged.
func (r *AggregatorReconciler) bakeFor(cfg []byte) time.Duration {
	var c struct {
		Sinks map[string]struct {
			Batch struct {
				TimeoutSecs float64 `json:"timeout_secs"`
			} `json:"batch"`
		} `json:"sinks"`
	}
	_ = yaml.Unmarshal(cfg, &c)
	longest := 0.0
	for _, sink := range c.Sinks {
		longest = max(longest, sink.Batch.TimeoutSecs)
	}
	return max(r.Canary.Bake, time.Duration(2*longest*float64(time.Second)))
}

// diskBufferedSinks returns the sinks in cfg with a disk buffer.
func diskBufferedSinks(cfg string) map[string]bool {
	var c struct {
		Sinks map[string]struct {
			Buffer struct {
				Type string `json:"type"`
			} `json:"buffer"`
		} `json:"sinks"`
	}
	_ = yaml.Unmarshal([]byte(cfg), &c)
	out := map[string]bool{}
	for id, sink := range c.Sinks {
		if sink.Buffer.Type == "disk" {
			out[id] = true
		}
	}
	return out
}

// rollout moves the aggregator towards running the config with hash, which
// renders pipelines.
func (r *AggregatorReconciler) rollout(ctx context.Context, sts *appsv1.StatefulSet, hash string, cfg []byte, pipelines []spillwayv1alpha1.LogPipeline) (rolloutState, ctrl.Result, error) {
	fp := make(map[string]int64, len(pipelines))
	for _, p := range pipelines {
		fp[string(p.UID)] = p.Generation
	}
	ann := sts.Annotations
	st := rolloutState{stable: map[string]int64{}}
	if s := ann[annStablePipelines]; s != "" {
		_ = json.Unmarshal([]byte(s), &st.stable)
	}
	stable, canary := ann[annStable], ann[annCanary]
	current := strings.TrimPrefix(sts.Spec.Template.Annotations[SuffixAnnotation], "-")

	switch {
	case canary != "" && canary == hash:
		return r.evaluateCanary(ctx, sts, st, r.bakeFor(cfg))

	case hash == stable && current != hash:
		// Back to the known-good config (a spec reverted mid-canary): no canary needed.
		return r.patchRollout(ctx, sts, st, hash, 0, map[string]*string{annCanary: nil, annCanaryStarted: nil, annCanaryBaseline: nil, annCanaryPipelines: nil},
			phaseRolling, "returning every aggregator to the stable config")

	case canary == "" && current != hash && hash == ann[annRejected]:
		st.phase = phaseRejected
		st.message = fmt.Sprintf("config %s was rolled back: %s. The aggregator runs the previous config; change the spec to try again", hash, ann[annRejectedReason])
		return st, ctrl.Result{}, nil

	case canary != "" || current != hash:
		// A new config, or a newer one replacing the canary in progress.
		replicas := replicasOf(sts)
		pod := fmt.Sprintf("%s-%d", sts.Name, replicas-1)
		log.FromContext(ctx).Info("starting canary", "hash", hash, "pod", pod, "replaces", canary)
		now := r.now().UTC().Format(time.RFC3339)
		raw, _ := json.Marshal(fp)
		fps := string(raw)
		return r.patchRollout(ctx, sts, st, hash, replicas-1,
			map[string]*string{annCanary: &hash, annCanaryStarted: &now, annCanaryBaseline: nil, annCanaryPipelines: &fps},
			phaseCanary, fmt.Sprintf("config %s is starting on canary pod %s", hash, pod))

	default:
		// Every pod has been told to run hash; wait for them, then record it as stable.
		if !fullyRolledOut(sts) {
			st.phase, st.message = phaseRolling, fmt.Sprintf("config %s passed its canary and is rolling to every aggregator", hash)
			return st, ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		if err := r.recordLastGoodSpecs(ctx, pipelines); err != nil {
			return st, ctrl.Result{}, err
		}
		if stable != hash {
			b, _ := json.Marshal(fp)
			fps := string(b)
			// Retried changes that are now stable have passed: release them.
			q := quarantineOf(sts)
			for uid, e := range q {
				if fp[uid] == e.Generation {
					delete(q, uid)
				}
			}
			qb, _ := json.Marshal(q)
			qs := string(qb)
			patch := client.MergeFrom(sts.DeepCopy())
			setAnnotations(&sts.Annotations, map[string]*string{annStable: &hash, annStablePipelines: &fps, annRejected: nil, annRejectedReason: nil, annQuarantine: &qs})
			if err := r.Patch(ctx, sts, patch); err != nil {
				return st, ctrl.Result{}, err
			}
			st.stable = fp
		}
		st.phase, st.message = phaseStable, "the aggregator is running config that includes this pipeline"
		return st, ctrl.Result{}, nil
	}
}

// evaluateCanary waits for the canary pod, bakes it, and promotes or rolls
// back.
func (r *AggregatorReconciler) evaluateCanary(ctx context.Context, sts *appsv1.StatefulSet, st rolloutState, bake time.Duration) (rolloutState, ctrl.Result, error) {
	ann := sts.Annotations
	hash := ann[annCanary]
	replicas := replicasOf(sts)
	canaryPod := fmt.Sprintf("%s-%d", sts.Name, replicas-1)
	started, _ := time.Parse(time.RFC3339, ann[annCanaryStarted])
	st.phase = phaseCanary

	if !canaryReady(sts) {
		if r.now().Sub(started) > r.Canary.ReadyTimeout {
			return r.rollback(ctx, sts, st, fmt.Sprintf("canary pod %s wasn't Ready within %s", canaryPod, r.Canary.ReadyTimeout))
		}
		st.message = fmt.Sprintf("config %s is starting on canary pod %s", hash, canaryPod)
		return st, ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Vector's metrics endpoint comes up a few seconds after the pod is Ready,
	// so a failed read is retried; the error goes to the log, not the status.
	c, err := r.Health.Check(ctx, r.Namespace, canaryPod)
	if err != nil {
		log.FromContext(ctx).V(1).Info("reading canary health", "pod", canaryPod, "error", err.Error())
		st.message = fmt.Sprintf("config %s is on canary pod %s; waiting for its metrics", hash, canaryPod)
		return st, ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	var s PodHealth
	stablePod := ""
	if replicas > 1 {
		stablePod = sts.Name + "-0"
		if s, err = r.Health.Check(ctx, r.Namespace, stablePod); err != nil {
			log.FromContext(ctx).Info("reading stable pod health", "pod", stablePod, "error", err.Error())
			st.message = fmt.Sprintf("config %s is on canary pod %s; waiting for stable pod %s's metrics", hash, canaryPod, stablePod)
			return st, ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}

	var b baseline
	if err := json.Unmarshal([]byte(ann[annCanaryBaseline]), &b); err != nil || b.At.IsZero() {
		b = baseline{At: r.now().UTC(), Bake: bake, Canary: c, Stable: s}
		raw, _ := json.Marshal(b)
		v := string(raw)
		patch := client.MergeFrom(sts.DeepCopy())
		setAnnotations(&sts.Annotations, map[string]*string{annCanaryBaseline: &v})
		if err := r.Patch(ctx, sts, patch); err != nil {
			return st, ctrl.Result{}, err
		}
		st.message = bakingMessage(hash, canaryPod, replicas, b)
		return st, ctrl.Result{RequeueAfter: min(b.Bake, 15*time.Second)}, nil
	}
	if b.Bake == 0 {
		b.Bake = bake // a baseline from before bakes were recorded
	}

	// Judge as soon as the canary misbehaves; promote only after the full bake.
	if c.Restarts > b.Canary.Restarts {
		return r.rollback(ctx, sts, st, fmt.Sprintf("canary pod %s restarted %d times", canaryPod, c.Restarts-b.Canary.Restarts))
	}
	canaryErrors, stableErrors := c.Errors-b.Canary.Errors, s.Errors-b.Stable.Errors
	events := 0.0
	for id, now := range c.Sinks {
		events += now.Received - b.Canary.Sinks[id].Received
	}
	allowance := max(r.Canary.ErrorAllowance, r.Canary.ErrorRatio*events)
	if canaryErrors-stableErrors > allowance {
		reason := fmt.Sprintf("canary pod %s logged %.0f errors in %s", canaryPod, canaryErrors, r.now().Sub(b.At).Round(time.Second))
		if stablePod != "" {
			reason += fmt.Sprintf(" against %.0f on stable pod %s", stableErrors, stablePod)
		}
		return r.rollback(ctx, sts, st, fmt.Sprintf("%s (allowed: %.0f more, for %.0f events)", reason, allowance, events))
	}
	if left := b.At.Add(b.Bake).Sub(r.now()); left > 0 {
		st.message = bakingMessage(hash, canaryPod, replicas, b)
		return st, ctrl.Result{RequeueAfter: min(left+time.Second, 15*time.Second)}, nil
	}
	// Judged only at the end: a sink may batch for up to a minute before it
	// sends, so a stall shows only over the whole bake.
	if stalled := stalledSinks(b.Canary, c, b.Stable, s); len(stalled) > 0 {
		return r.rollback(ctx, sts, st, fmt.Sprintf("canary pod %s took in events for %s but delivered none in %s",
			canaryPod, strings.Join(stalled, ", "), r.now().Sub(b.At).Round(time.Second)))
	}

	log.FromContext(ctx).Info("canary passed; promoting", "hash", hash, "canaryErrors", canaryErrors, "stableErrors", stableErrors)
	rollouts.WithLabelValues("promoted").Inc()
	return r.patchRollout(ctx, sts, st, hash, 0, map[string]*string{annCanary: nil, annCanaryStarted: nil, annCanaryBaseline: nil, annCanaryPipelines: nil},
		phaseRolling, fmt.Sprintf("config %s passed its canary and is rolling to every aggregator", hash))
}

func bakingMessage(hash, pod string, replicas int32, b baseline) string {
	return fmt.Sprintf("canary: config %s is running on %s (1 of %d aggregators); promoting at %s if its error rate holds",
		hash, pod, replicas, b.At.Add(b.Bake).UTC().Format(time.RFC3339))
}

// rollback returns the canary pod to the stable config. The pipeline
// generations the canary introduced (those not in the stable config) are
// quarantined: later configs leave them out until their spec changes, so
// other teams' changes can still roll out. If the canary introduced no
// pipeline changes, its config hash is recorded instead, so it isn't retried.
func (r *AggregatorReconciler) rollback(ctx context.Context, sts *appsv1.StatefulSet, st rolloutState, reason string) (rolloutState, ctrl.Result, error) {
	hash := sts.Annotations[annCanary]
	log.FromContext(ctx).Info("canary failed; rolling back", "hash", hash, "reason", reason)
	rollouts.WithLabelValues("rolled_back").Inc()

	var canaryFP map[string]int64
	_ = json.Unmarshal([]byte(sts.Annotations[annCanaryPipelines]), &canaryFP)
	var suspects []string
	for uid, gen := range canaryFP {
		if stableGen, ok := st.stable[uid]; !ok || stableGen != gen {
			suspects = append(suspects, uid)
		}
	}
	// With one change, it's at fault. With several, none is known to be:
	// each is retried in a canary of its own, and only those that fail alone
	// stay quarantined.
	q := quarantineOf(sts)
	for _, uid := range suspects {
		q[uid] = quarantined{Generation: canaryFP[uid], Reason: fmt.Sprintf("config %s was rolled back: %s", hash, reason), Retry: len(suspects) > 1}
	}
	orphans := r.orphanedBuffers(ctx, hash, sts.Annotations[annStable], fmt.Sprintf("%s-%d", sts.Name, replicasOf(sts)-1))
	raw, _ := json.Marshal(q)
	qs := string(raw)
	annotations := map[string]*string{annCanary: nil, annCanaryStarted: nil, annCanaryBaseline: nil, annCanaryPipelines: nil,
		annQuarantine: &qs, annRejectedReason: &reason}
	if len(suspects) == 0 {
		annotations[annRejected] = &hash
	}
	st, res, err := r.patchRollout(ctx, sts, st, sts.Annotations[annStable], 0, annotations, phaseRejected, "")
	st.message = fmt.Sprintf("config %s was rolled back: %s. The aggregator runs the previous config", hash, reason)
	if len(orphans) > 0 {
		st.message += fmt.Sprintf("; events %s buffered on the canary pod weren't delivered (issue #43)", strings.Join(orphans, ", "))
	}
	return st, res, err
}

// orphanedBuffers reports the disk-buffered sinks the rolled-back config
// added: the stable config has no such sink, so whatever the canary pod
// buffered in them is never read (issue #43). It logs and counts them.
func (r *AggregatorReconciler) orphanedBuffers(ctx context.Context, canary, stable, pod string) []string {
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: r.ConfigMap}, &cm); err != nil {
		return nil
	}
	stableKey := BootstrapKey
	if stable != "" {
		stableKey = configKey(stable)
	}
	before := diskBufferedSinks(cm.Data[stableKey])
	var out []string
	for id := range diskBufferedSinks(cm.Data[configKey(canary)]) {
		if !before[id] {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	for _, id := range out {
		orphanedBuffers.WithLabelValues(id).Inc()
		log.FromContext(ctx).Info("WARNING: rollback leaves a disk buffer nothing reads; its events weren't delivered (issue #43)",
			"pod", pod, "sink", id, "config", canary)
	}
	return out
}

// quarantineOf reads the quarantined pipeline generations.
func quarantineOf(sts *appsv1.StatefulSet) map[string]quarantined {
	q := map[string]quarantined{}
	_ = json.Unmarshal([]byte(sts.Annotations[annQuarantine]), &q)
	return q
}

// patchRollout points the pod template at the config with hash (empty means
// the bootstrap config), sets the partition, and updates the StatefulSet's
// rollout annotations, in one patch.
func (r *AggregatorReconciler) patchRollout(ctx context.Context, sts *appsv1.StatefulSet, st rolloutState,
	hash string, partition int32, annotations map[string]*string, p phase, message string) (rolloutState, ctrl.Result, error) {
	patch := client.MergeFrom(sts.DeepCopy())
	suffix := ""
	if hash != "" {
		suffix = "-" + hash
	}
	setAnnotations(&sts.Spec.Template.Annotations, map[string]*string{SuffixAnnotation: &suffix})
	setAnnotations(&sts.Annotations, annotations)
	sts.Spec.UpdateStrategy.Type = appsv1.RollingUpdateStatefulSetStrategyType
	if sts.Spec.UpdateStrategy.RollingUpdate == nil {
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
	}
	sts.Spec.UpdateStrategy.RollingUpdate.Partition = &partition
	if err := r.Patch(ctx, sts, patch); err != nil {
		return st, ctrl.Result{}, err
	}
	st.phase, st.message = p, message
	return st, ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// setAnnotations sets keys with a value and deletes keys mapped to nil.
func setAnnotations(m *map[string]string, kv map[string]*string) {
	if *m == nil {
		*m = map[string]string{}
	}
	for k, v := range kv {
		if v == nil {
			delete(*m, k)
		} else {
			(*m)[k] = *v
		}
	}
}

func replicasOf(sts *appsv1.StatefulSet) int32 {
	if sts.Spec.Replicas != nil {
		return *sts.Spec.Replicas
	}
	return 1
}

func partitionOf(sts *appsv1.StatefulSet) int32 {
	if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil {
		return *ru.Partition
	}
	return 0
}

// canaryReady reports whether the pods at or above the partition run the
// current template and every pod is Ready.
func canaryReady(sts *appsv1.StatefulSet) bool {
	s, replicas := sts.Status, replicasOf(sts)
	return s.ObservedGeneration >= sts.Generation && s.UpdateRevision != "" &&
		s.UpdatedReplicas >= replicas-partitionOf(sts) && s.ReadyReplicas == replicas
}

// fullyRolledOut reports whether every pod runs the current template.
func fullyRolledOut(sts *appsv1.StatefulSet) bool {
	s, replicas := sts.Status, replicasOf(sts)
	return partitionOf(sts) == 0 && s.ObservedGeneration >= sts.Generation &&
		s.UpdateRevision != "" && s.CurrentRevision == s.UpdateRevision &&
		s.UpdatedReplicas == replicas && s.ReadyReplicas == replicas
}

// selectsConfig reports whether the aggregator's container picks its config
// file from the suffix annotation; without that, revisions can't differ per pod.
func selectsConfig(sts *appsv1.StatefulSet) bool {
	for _, c := range sts.Spec.Template.Spec.Containers {
		hasEnv := false
		for _, e := range c.Env {
			if e.Name == SuffixEnv && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil &&
				e.ValueFrom.FieldRef.FieldPath == "metadata.annotations['"+SuffixAnnotation+"']" {
				hasEnv = true
			}
		}
		if hasEnv && strings.Contains(strings.Join(c.Args, " "), "vector$("+SuffixEnv+").yaml") {
			return true
		}
	}
	return false
}
