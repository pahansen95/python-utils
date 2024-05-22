# Python Utils

A set of utilities for faster Python development.

Supports (Best Effort & Untested) the 2 latest minor versions of Python & Linux.

## Building

General Steps:

- Write the Configuration
  - Write the Package Spec
  - Write the Artifact Spec
- Run the Build Script

> NOTES:
>
> - See the [quickstart](./quickstart.sh) script for an example.

This project supports distributing the package as various Artifacts formats:

- A directory tree & requirements file (for inclusion as part of another project/package)
- (TODO) A Pip Package (ie. wheel, sdist, etc...)

### Configs

Configurations define what is built & bundled. See the [build script](./build/__main__.py) for a Configuration Specification.

There are a handful of configurations provided as starting points:

| Path | Description |
| - | - |
| [config/stable/](config/stable/) | Configurations scoped to Stable Utilities |
| [config/stable/all.yaml](./config/stable/all.yaml) | All stable utils |
| [config/stable/webapp.yaml](./config/stable/minimal.yaml) | A set of stable utils for building WebApps |
| [config/stable/minimal.yaml](./config/stable/minimal.yaml) | The set of stable utils I most commonly use |

> NOTES:
> 
> - See the [config](./config/) directory tree for a full list of configurations
