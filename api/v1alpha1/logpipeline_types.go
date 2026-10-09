package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Validation lives in the markers below, so the API server rejects a
// malformed spec when it's applied, before the operator ever sees it.

// LogPipelineSpec is one team's pipeline: where its logs come from and the
// policy applied to them before they're stored.
type LogPipelineSpec struct {
	// Team owns the pipeline. It labels the team's logs and metrics and names
	// its cold-storage prefix, so it must be a DNS label.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Team string `json:"team"`

	// Sources the team's logs are read from.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +listType=map
	// +listMapKey=name
	Sources []Source `json:"sources"`

	// Redaction masks PII before logs leave the aggregator.
	// +optional
	Redaction *Redaction `json:"redaction,omitempty"`

	// Sampling keeps a share of events per level; levels not listed are kept.
	// +optional
	Sampling *Sampling `json:"sampling,omitempty"`

	// Budget caps the team's event rate. Events over budget are counted and
	// dropped, never silently lost.
	// +optional
	Budget *Budget `json:"budget,omitempty"`

	// Routing picks the sinks: hot (Loki, queryable) and cold (object storage).
	// +kubebuilder:default={hot: true}
	// +optional
	Routing *Routing `json:"routing,omitempty"`
}

// Source is one input. Exactly one of its source types must be set.
// +kubebuilder:validation:XValidation:rule="has(self.kafka) != has(self.kubernetes)",message="exactly one of kafka or kubernetes must be set"
type Source struct {
	// Name identifies the source within the pipeline.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// Kafka reads a topic, such as one a feeder produces to.
	// +optional
	Kafka *KafkaSource `json:"kafka,omitempty"`

	// Kubernetes reads the logs of pods in the given namespaces, collected by
	// the agent DaemonSet.
	// +optional
	Kubernetes *KubernetesSource `json:"kubernetes,omitempty"`
}

// KafkaSource reads one Kafka topic.
type KafkaSource struct {
	// Topic to consume.
	// +kubebuilder:validation:MaxLength=249
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9._-]+$`
	Topic string `json:"topic"`
}

// KubernetesSource reads pod logs from namespaces.
type KubernetesSource struct {
	// Namespaces whose pod logs belong to this pipeline.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +listType=set
	Namespaces []string `json:"namespaces"`
}

// RedactionPattern is a built-in PII pattern.
// +kubebuilder:validation:Enum=ssn;email;phone;memberId
type RedactionPattern string

const (
	RedactSSN      RedactionPattern = "ssn"
	RedactEmail    RedactionPattern = "email"
	RedactPhone    RedactionPattern = "phone"
	RedactMemberID RedactionPattern = "memberId"
)

// Redaction lists the patterns to mask.
type Redaction struct {
	// +kubebuilder:validation:MinItems=1
	// +listType=set
	Patterns []RedactionPattern `json:"patterns"`
}

// Percent is a whole percentage from 0 to 100.
// +kubebuilder:validation:Minimum=0
// +kubebuilder:validation:Maximum=100
type Percent int32

// Sampling keeps a percentage of events per log level.
type Sampling struct {
	// LevelField is the event field holding the log level, as a dot-separated
	// path such as "level" or "log.level".
	// +kubebuilder:default=level
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_@-]+(\.[A-Za-z0-9_@-]+)*$`
	// +optional
	LevelField string `json:"levelField,omitempty"`

	// KeepPercent maps a level to the share of its events kept, e.g.
	// {debug: 1, info: 25}. 0 drops the level; 100 keeps all of it.
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=16
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[a-z][a-z0-9_-]{0,31}$'))",message="levels must be lowercase words of at most 32 characters"
	KeepPercent map[string]Percent `json:"keepPercent"`
}

// Budget limits a team's event rate.
type Budget struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	MaxEventsPerSec int32 `json:"maxEventsPerSec"`
}

// Routing chooses where the team's logs are stored. At least one sink must be
// enabled.
// +kubebuilder:validation:XValidation:rule="(has(self.hot) && self.hot) || (has(self.cold) && self.cold)",message="at least one of hot or cold must be enabled"
type Routing struct {
	// Hot sends logs to Loki, where they're queryable in Grafana.
	// +kubebuilder:default=true
	// +optional
	Hot *bool `json:"hot,omitempty"`

	// Cold sends logs to object storage, partitioned by team and date.
	// +kubebuilder:default=false
	// +optional
	Cold *bool `json:"cold,omitempty"`
}

// LogPipelineStatus is the operator's view of the pipeline.
type LogPipelineStatus struct {
	// ObservedGeneration is the spec generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions: Ready is True once the aggregator runs config that includes
	// this pipeline's current generation. When False, its reason says why:
	// RollingOut while new config is on a canary or rolling to every
	// aggregator, CanaryFailed when the canary was rolled back (the aggregator
	// keeps the previous config), Invalid when the pipeline can't be
	// rendered (for example, its team or a namespace is already claimed) or
	// Vector rejects its config, ValidationFailed when the combined config is
	// rejected and the aggregator keeps its last valid config, or
	// AggregatorNotFound or AggregatorMisconfigured.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// LogPipeline is one team's log pipeline.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=lp
// +kubebuilder:printcolumn:name="Team",type=string,JSONPath=`.spec.team`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Message",type=string,priority=1,JSONPath=`.status.conditions[?(@.type=="Ready")].message`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type LogPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LogPipelineSpec   `json:"spec"`
	Status LogPipelineStatus `json:"status,omitempty"`
}

// LogPipelineList is a list of LogPipelines.
// +kubebuilder:object:root=true
type LogPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LogPipeline `json:"items"`
}

// The condition type and reasons the operator sets.
const (
	ConditionReady = "Ready"

	ReasonRolledOut               = "RolledOut"
	ReasonRollingOut              = "RollingOut"
	ReasonInvalid                 = "Invalid"
	ReasonValidationFailed        = "ValidationFailed"
	ReasonAggregatorNotFound      = "AggregatorNotFound"
	ReasonAggregatorMisconfigured = "AggregatorMisconfigured"
	ReasonCanaryFailed            = "CanaryFailed"
)
