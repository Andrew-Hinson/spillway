package validate

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	"github.com/Andrew-Hinson/spillway/internal/render"
)

// Any spec the CRD accepts must render to config Vector accepts. This
// generates random specs within the CRD's rules (leaning on the edges: names
// of length 1 and 63, digits first, dots-only topics, deep level paths) and
// runs the real `vector validate` on each render. The seed is fixed, so a
// failure reproduces.
func TestEverySchemaValidSpecRendersValidConfig(t *testing.T) {
	bin := vectorBin(t)
	rng := rand.New(rand.NewPCG(2026, 10))
	v := Vector{Bin: bin}
	validated := 0
	for i := range 40 {
		// One to three teams, so shared sinks and platform filters are exercised.
		var pipelines []spillwayv1alpha1.LogPipeline
		for j := range 1 + rng.IntN(3) {
			pipelines = append(pipelines, randomPipeline(rng, fmt.Sprintf("p%d-%d", i, j)))
		}
		cfg, err := render.Render(pipelines, render.DefaultOptions())
		if err != nil {
			continue // a cross-object conflict, which the operator marks Invalid
		}
		if err := v.Validate(context.Background(), cfg); err != nil {
			var verdict *Error
			if errors.As(err, &verdict) {
				t.Fatalf("case %d: schema-valid specs rendered config Vector rejects:\n%s\n--- specs ---\n%s", i, verdict.Output, describe(pipelines))
			}
			t.Fatal(err)
		}
		validated++
	}
	// Random names rarely collide, so nearly every case should reach Vector.
	if validated < 35 {
		t.Fatalf("only %d of 40 cases rendered; the generator is producing conflicts, not tests", validated)
	}
	t.Logf("%d random spec sets rendered and validated", validated)
}

const (
	lower  = "abcdefghijklmnopqrstuvwxyz"
	digits = "0123456789"
)

func pick(rng *rand.Rand, chars string, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rng.IntN(len(chars))]
	}
	return string(b)
}

// dnsLabel matches ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$, max 63.
func dnsLabel(rng *rand.Rand) string {
	n := []int{1, 2, 63, 1 + rng.IntN(20)}[rng.IntN(4)]
	if n == 1 {
		return pick(rng, lower+digits, 1)
	}
	return pick(rng, lower+digits, 1) + pick(rng, lower+digits+"-", n-2) + pick(rng, lower+digits, 1)
}

func randomPipeline(rng *rand.Rand, name string) spillwayv1alpha1.LogPipeline {
	lp := spillwayv1alpha1.LogPipeline{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name}}
	lp.Spec.Team = dnsLabel(rng)
	seen := map[string]bool{}
	for range 1 + rng.IntN(3) {
		s := spillwayv1alpha1.Source{Name: dnsLabel(rng)}
		if seen[s.Name] {
			continue
		}
		seen[s.Name] = true
		if rng.IntN(2) == 0 {
			topics := []string{".", "..", "_", "a", pick(rng, lower+digits+"._-ABCXYZ", 1+rng.IntN(60)), "wikimedia.recentchange"}
			s.Kafka = &spillwayv1alpha1.KafkaSource{Topic: topics[rng.IntN(len(topics))]}
		} else {
			s.Kubernetes = &spillwayv1alpha1.KubernetesSource{Namespaces: []string{dnsLabel(rng), dnsLabel(rng)}}
			if s.Kubernetes.Namespaces[0] == s.Kubernetes.Namespaces[1] {
				s.Kubernetes.Namespaces = s.Kubernetes.Namespaces[:1]
			}
		}
		lp.Spec.Sources = append(lp.Spec.Sources, s)
	}
	if rng.IntN(2) == 0 {
		all := []spillwayv1alpha1.RedactionPattern{spillwayv1alpha1.RedactSSN, spillwayv1alpha1.RedactEmail, spillwayv1alpha1.RedactPhone, spillwayv1alpha1.RedactMemberID}
		rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
		lp.Spec.Redaction = &spillwayv1alpha1.Redaction{Patterns: all[:1+rng.IntN(len(all))]}
	}
	if rng.IntN(2) == 0 {
		var parts []string
		for range 1 + rng.IntN(4) {
			parts = append(parts, pick(rng, lower+digits+"_@-ABC", 1+rng.IntN(8)))
		}
		keep := map[string]spillwayv1alpha1.Percent{}
		for range 1 + rng.IntN(16) {
			keep[pick(rng, lower, 1)+pick(rng, lower+digits+"_-", rng.IntN(32))] = spillwayv1alpha1.Percent(rng.IntN(101))
		}
		lp.Spec.Sampling = &spillwayv1alpha1.Sampling{LevelField: strings.Join(parts, "."), KeepPercent: keep}
	}
	if rng.IntN(2) == 0 {
		lp.Spec.Budget = &spillwayv1alpha1.Budget{MaxEventsPerSec: []int32{1, 1000000, 1 + rng.Int32N(5000)}[rng.IntN(3)]}
	}
	hot, cold := true, rng.IntN(2) == 0
	if cold && rng.IntN(2) == 0 {
		hot = false
	}
	lp.Spec.Routing = &spillwayv1alpha1.Routing{Hot: &hot, Cold: &cold}
	return lp
}

func describe(pipelines []spillwayv1alpha1.LogPipeline) string {
	var b strings.Builder
	for _, p := range pipelines {
		fmt.Fprintf(&b, "%s: %+v\n", p.Name, p.Spec)
	}
	return b.String()
}
