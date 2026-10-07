// Package schema validates LogPipeline manifests against the generated CRD
// without a cluster, the way the API server does on `kubectl apply`: unknown
// fields are rejected, defaults are applied, then the OpenAPI schema, list
// uniqueness and the CEL rules are checked. It uses the API server's own validation libraries
// and the embedded CRD, so the verdicts match.
package schema

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	"github.com/Andrew-Hinson/spillway/deploy/operator/crd"
)

// Validator checks manifests against one version of the LogPipeline CRD.
type Validator struct {
	structural *structuralschema.Structural
	openapi    apiservervalidation.SchemaValidator
	cel        *cel.Validator
}

// New builds a Validator from the embedded CRD.
func New() (*Validator, error) {
	var c apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(crd.LogPipeline, &c); err != nil {
		return nil, fmt.Errorf("parsing the embedded CRD: %w", err)
	}
	i := slices.IndexFunc(c.Spec.Versions, func(v apiextensionsv1.CustomResourceDefinitionVersion) bool {
		return v.Name == spillwayv1alpha1.GroupVersion.Version
	})
	if i < 0 || c.Spec.Versions[i].Schema == nil {
		return nil, fmt.Errorf("the embedded CRD has no %s schema", spillwayv1alpha1.GroupVersion.Version)
	}
	var props apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
		c.Spec.Versions[i].Schema.OpenAPIV3Schema, &props, nil); err != nil {
		return nil, err
	}
	s, err := structuralschema.NewStructural(&props)
	if err != nil {
		return nil, err
	}
	openapi, _, err := apiservervalidation.NewSchemaValidator(&props)
	if err != nil {
		return nil, err
	}
	return &Validator{structural: s, openapi: openapi, cel: cel.NewValidator(s, true, celconfig.PerCallLimit)}, nil
}

// Validate checks one manifest. It returns the object with the CRD's defaults
// applied, and every problem found (empty if the API server would accept it).
func (v *Validator) Validate(ctx context.Context, manifest []byte) (map[string]any, field.ErrorList) {
	var obj map[string]any
	if err := yaml.Unmarshal(manifest, &obj); err != nil {
		return nil, field.ErrorList{field.Invalid(nil, nil, "not valid YAML: "+err.Error())}
	}
	if obj == nil {
		return nil, field.ErrorList{field.Required(nil, "empty document")}
	}
	if obj["apiVersion"] != spillwayv1alpha1.GroupVersion.String() || obj["kind"] != "LogPipeline" {
		return nil, field.ErrorList{field.Invalid(field.NewPath("kind"), fmt.Sprintf("%v %v", obj["apiVersion"], obj["kind"]),
			"want apiVersion "+spillwayv1alpha1.GroupVersion.String()+", kind LogPipeline")}
	}

	var errs field.ErrorList
	// Unknown fields, as `kubectl apply` (strict field validation) reports them.
	unknown := pruning.PruneWithOptions(obj, v.structural, true, structuralschema.UnknownFieldPathOptions{TrackUnknownFieldPaths: true})
	for _, path := range unknown {
		errs = append(errs, field.Invalid(nil, nil, fmt.Sprintf("unknown field %q", path)))
	}
	meta, _ := obj["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	if name == "" {
		errs = append(errs, field.Required(field.NewPath("metadata", "name"), ""))
	} else {
		for _, msg := range validation.NameIsDNSSubdomain(name, false) {
			errs = append(errs, field.Invalid(field.NewPath("metadata", "name"), name, msg))
		}
	}

	defaulting.Default(obj, v.structural)
	errs = append(errs, apiservervalidation.ValidateCustomResource(nil, obj, v.openapi)...)
	// Unique keys in listType=map and items in listType=set lists.
	errs = append(errs, listtype.ValidateListSetsAndMaps(nil, v.structural, obj)...)
	celErrs, _ := v.cel.Validate(ctx, nil, v.structural, obj, nil, celconfig.RuntimeCELCostBudget)
	errs = append(errs, celErrs...)
	return obj, errs
}
