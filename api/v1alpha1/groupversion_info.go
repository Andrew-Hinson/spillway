// Package v1alpha1 holds the LogPipeline API: one spec per team describing
// where its logs come from and what policy runs on them before storage.
// +kubebuilder:object:generate=true
// +groupName=spillway.dev
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group and version of the LogPipeline API.
	GroupVersion = schema.GroupVersion{Group: "spillway.dev", Version: "v1alpha1"}

	// SchemeBuilder registers the API types with a runtime.Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the API types to a runtime.Scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &LogPipeline{}, &LogPipelineList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
