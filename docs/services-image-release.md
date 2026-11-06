# Releasing the services image

Use the manual `publish-image` workflow after the operator approves the source
commit and publication. The workflow still requires Khaliq's GitHub identity,
the `main` workflow ref, the confirmation phrase and the `image-publish`
environment. Approval to merge code does not authorize publication.

Set `source_sha` to the approved full lowercase public commit SHA. It must
belong to current `main`, and its latest official main/push CI run and every job
must be successful. The preflight reads metadata with a read-only token and
fails before the registry/OIDC job if the source, CI, dispatcher or approval
reference is invalid. It rechecks the CI attempt after reading its jobs.

The input is required: tags, branch names, abbreviated SHAs and internal-history
commits are refused. Only `sha-<source_sha>` is published. This publisher never
moves `main` or `latest` across the separate source histories. Supply a short,
single-line, non-sensitive approval reference; it becomes public image metadata.

The target remains `ghcr.io/agentworkforce/96bulls-services`. The new public
repository needs Actions write access to that existing package. Package access
and package/repository association are separate settings; do not assume public
pull access authorizes a workflow to push. A denied publish is a permission
failure to resolve with the package owner, not a reason to use another token or
disable release gates.

The workflow builds amd64 and arm64 images, generates an SBOM and max-mode
BuildKit provenance, and signs two statements for the published digest:

- GitHub build provenance identifies the workflow and its commit.
- `https://agentrelay.com/attestations/reviewed-image-source/v1` records the
  checked-out `source_sha`, its repository, its verified membership on `main`,
  the `workflow_sha`, and the successful `ci_run_id`. These commits can differ for a pinned release.

## Verify a release

Obtain `image` (including its `sha256:` digest), `source_sha` and `workflow_sha`
from the reviewed release receipt. Verify both the trusted workflow identity
and the source statement before promoting the image:

```bash
gh attestation verify "oci://$image" \
  --repo AgentWorkforce/sessions-pipeline-oss \
  --signer-workflow AgentWorkforce/sessions-pipeline-oss/.github/workflows/publish-image.yml \
  --signer-digest "$workflow_sha" --source-ref refs/heads/main

gh attestation verify "oci://$image" \
  --repo AgentWorkforce/sessions-pipeline-oss \
  --signer-workflow AgentWorkforce/sessions-pipeline-oss/.github/workflows/publish-image.yml \
  --signer-digest "$workflow_sha" --source-ref refs/heads/main \
  --predicate-type https://agentrelay.com/attestations/reviewed-image-source/v1 \
  --format json > source-verification.json

jq -e --arg source "$source_sha" --arg workflow "$workflow_sha" '
  any(.[]; .verificationResult.statement.predicate |
    .repository == "https://github.com/AgentWorkforce/sessions-pipeline-oss" and
    .source_ref == "refs/heads/main" and
    .source_sha == $source and .workflow_sha == $workflow)
' source-verification.json
```

GitHub's certificate source digest describes the workflow run's source, so do
not substitute the image's older `source_sha` for `workflow_sha` when checking
the signer. Source metadata is a claim made by the trusted workflow; the
signed statement does not replace reviewing that workflow's checkout and
ancestry checks.

Inspect both platform manifests and their SBOMs, then smoke-test the published
digest. Pin that digest in deployment configuration. Publication alone does
not upgrade a Helm release or apply database migrations.

## Initial OSS release

The reviewed import source is `b81457d35f8f0764af69970f3856230a7f161243`,
whose main CI run is `42139828471`. Its local multiarch OCI candidate is not a
signed registry release. Dispatch the new public workflow after its publisher
change is integrated and the operator authorizes publication:

```sh
gh workflow run publish-image.yml --repo AgentWorkforce/sessions-pipeline-oss \
  --ref main \
  -f source_sha=b81457d35f8f0764af69970f3856230a7f161243 \
  -f confirm='publish 96bulls-services' \
  -f ref_note='<short public approval reference>'
```

The dispatch must use Khaliq's GitHub identity. Record the actual workflow SHA,
source CI run, resulting digest, verification results and runtime smoke results.
A push followed by a signing failure leaves an unpromoted candidate; it is not
a verified release. Do not update Helm image pins until verification succeeds.
