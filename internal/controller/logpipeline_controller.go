// Package controller reconciles LogPipelines into running Vector config.
package controller

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
)

// LogPipelineReconciler watches LogPipelines. Rendering, validation and
// rollout come in M2.2-M2.4; for now it only observes them.
type LogPipelineReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=spillway.dev,resources=logpipelines,verbs=get;list;watch
// +kubebuilder:rbac:groups=spillway.dev,resources=logpipelines/status,verbs=get;update;patch

// Reconcile is called for every change to a LogPipeline.
func (r *LogPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var lp spillwayv1alpha1.LogPipeline
	if err := r.Get(ctx, req.NamespacedName, &lp); err != nil {
		// Deleted since the event was queued: nothing to do.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.FromContext(ctx).Info("observed LogPipeline",
		"team", lp.Spec.Team, "sources", len(lp.Spec.Sources), "generation", lp.Generation)
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler with the manager.
func (r *LogPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&spillwayv1alpha1.LogPipeline{}).
		Named("logpipeline").
		Complete(r)
}
