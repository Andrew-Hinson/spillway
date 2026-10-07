Malformed LogPipeline specs, one per file. Each starts with `# want: <text>`,
a substring of the error it must be rejected with. The same files are checked
against a real API server (api/v1alpha1) and against `spillwayctl validate`'s
offline schema check (internal/schema), so the two can't drift apart.
