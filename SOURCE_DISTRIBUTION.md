# Source distribution boundary

This repository starts with a clean engine snapshot under Apache-2.0. It does not import the internal repository's Git history, production plan captures, deployment records, account configuration or publishing/deployment workflows.

The import baseline is `27c596d646d8b1f087dece21f12d4db1fb5c1466`, which contains the reviewed worker-runtime implementation from `f16d2cc721e59315f297e82c5fb5d2a13e953665` and the subsequent release/CI preparation. This is a fixed source snapshot, not a claim to mirror every later internal change.

The runtime services, pipeline and storage contracts, migrations, role bootstrap SQL, schema compatibility fixtures and their portable tests are retained. Migration SQL is unchanged from that baseline. Existing package names and service entrypoint values are preserved for compatibility. The former hosted canonical-read adapter is retained only as a parity-test fixture; its hosted Worker configuration and deployment package are excluded.

Hosted infrastructure stacks, Pippen orchestration, production owner-apply entrypoints and operations captures remain with private deployment composition. Tests for excluded deployment components remain there too. In particular, the assertion about credential scoping in the private `apply-session-schema.yml` workflow accompanies that excluded workflow. Portable database guard, TLS, tenant isolation and migration tests remain in this distribution.

Example account/project/host pins replace internal environment identifiers. Experimental hosted publication/GC and owner-operation tooling is not a ready customer deployment profile. No deployment or provider activation occurs merely by cloning or building this source.

The public CI workflow uses public package dependencies and disposable test services. It has no publishing or deployment job. Images must be built and verified against this repository's actual commit before release; an image built from an earlier internal commit must not be relabeled as built from this source.

Future source updates should be reviewed changes against this repository. Do not mirror or force-push internal branches or history into it.
