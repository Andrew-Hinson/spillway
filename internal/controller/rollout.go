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
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"

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
}

// configKey is the ConfigMap key for a config revision.
func configKey(hash string) string { return "vector-" + hash + ".yaml" }

// CanarySettings tune the canary.
type CanarySettings struct {
	// Bake is how long the canary runs, once Ready, before it's judged.
	Bake time.Duration
	// ReadyTimeout is how long the canary pod may take to become Ready.
	ReadyTimeout time.Duration
	// ErrorAllowance is how many more errors than the stable pod the canary
	// may log during the bake.
	ErrorAllowance float64
}

var rollouts = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "spillway_operator_rollouts_total",
	Help: "Aggregator config canaries, by result (promoted or rolled_back).",
}, []string{"result"})

func init() { metrics.Registry.MustRegister(rollouts) }

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
	At     time.Time `json:"at"`
	Canary PodHealth `json:"canary"`
	Stable PodHealth `json:"stable"`
}

// rollout moves the aggregator towards running the config with hash, which
// renders pipelines.
func (r *AggregatorReconciler) rollout(ctx context.Context, sts *appsv1.StatefulSet, hash string, pipelines []spillwayv1alpha1.LogPipeline) (rolloutState, ctrl.Result, error) {
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
		return r.evaluateCanary(ctx, sts, st)

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
			patch := client.MergeFrom(sts.DeepCopy())
			setAnnotations(&sts.Annotations, map[string]*string{annStable: &hash, annStablePipelines: &fps, annRejected: nil, annRejectedReason: nil})
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
func (r *AggregatorReconciler) evaluateCanary(ctx context.Context, sts *appsv1.StatefulSet, st rolloutState) (rolloutState, ctrl.Result, error) {
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
		b = baseline{At: r.now().UTC(), Canary: c, Stable: s}
		raw, _ := json.Marshal(b)
		v := string(raw)
		patch := client.MergeFrom(sts.DeepCopy())
		setAnnotations(&sts.Annotations, map[string]*string{annCanaryBaseline: &v})
		if err := r.Patch(ctx, sts, patch); err != nil {
			return st, ctrl.Result{}, err
		}
		st.message = r.bakingMessage(hash, canaryPod, replicas, b.At)
		return st, ctrl.Result{RequeueAfter: r.Canary.Bake}, nil
	}

	// Judge as soon as the canary misbehaves; promote only after the full bake.
	if c.Restarts > b.Canary.Restarts {
		return r.rollback(ctx, sts, st, fmt.Sprintf("canary pod %s restarted %d times", canaryPod, c.Restarts-b.Canary.Restarts))
	}
	canaryErrors, stableErrors := c.Errors-b.Canary.Errors, s.Errors-b.Stable.Errors
	if canaryErrors-stableErrors > r.Canary.ErrorAllowance {
		reason := fmt.Sprintf("canary pod %s logged %.0f errors in %s", canaryPod, canaryErrors, r.now().Sub(b.At).Round(time.Second))
		if stablePod != "" {
			reason += fmt.Sprintf(" against %.0f on stable pod %s", stableErrors, stablePod)
		}
		return r.rollback(ctx, sts, st, reason)
	}
	if left := b.At.Add(r.Canary.Bake).Sub(r.now()); left > 0 {
		st.message = r.bakingMessage(hash, canaryPod, replicas, b.At)
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

func (r *AggregatorReconciler) bakingMessage(hash, pod string, replicas int32, from time.Time) string {
	return fmt.Sprintf("canary: config %s is running on %s (1 of %d aggregators); promoting at %s if its error rate holds",
		hash, pod, replicas, from.Add(r.Canary.Bake).UTC().Format(time.RFC3339))
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
	q := quarantineOf(sts)
	suspects := 0
	for uid, gen := range canaryFP {
		if stableGen, ok := st.stable[uid]; !ok || stableGen != gen {
			q[uid] = quarantined{Generation: gen, Reason: fmt.Sprintf("config %s was rolled back: %s", hash, reason)}
			suspects++
		}
	}
	raw, _ := json.Marshal(q)
	qs := string(raw)
	annotations := map[string]*string{annCanary: nil, annCanaryStarted: nil, annCanaryBaseline: nil, annCanaryPipelines: nil,
		annQuarantine: &qs, annRejectedReason: &reason}
	if suspects == 0 {
		annotations[annRejected] = &hash
	}
	st, res, err := r.patchRollout(ctx, sts, st, sts.Annotations[annStable], 0, annotations, phaseRejected, "")
	st.message = fmt.Sprintf("config %s was rolled back: %s. The aggregator runs the previous config", hash, reason)
	return st, res, err
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
