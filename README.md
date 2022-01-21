<p align="center">
  <h1 align="center">
    merge-counts
  </h1>

  <p align="center">
    <a href="https://actions-badge.atrox.dev/stjudecloud/merge-counts/goto" target="_blank">
      <img alt="Actions: CI Status"
          src="https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2Fstjudecloud%2Fmerge-counts%2Fbadge&style=flat" />
    </a>
    <a href="https://pypi.org/project/stjudecloud-merge-counts/" target="_blank">
      <img alt="PyPI"
          src="https://img.shields.io/pypi/v/stjudecloud-merge-counts?color=orange">
    </a>
    <a href="https://pypi.python.org/pypi/stjudecloud-merge-counts/" target="_blank">
      <img alt="PyPI: Downloads"
          src="https://img.shields.io/pypi/dm/stjudecloud-merge-counts?color=orange">
    </a>
    <a href="https://pypi.python.org/pypi/stjudecloud-merge-counts/" target="_blank">
      <img alt="PyPI: Downloads"
          src="https://img.shields.io/pypi/pyversions/stjudecloud-merge-counts?color=orange">
    </a>
    <a href="https://github.com/stjudecloud/merge-counts/blob/master/LICENSE.md" target="_blank">
    <img alt="License: MIT"
          src="https://img.shields.io/badge/License-MIT-blue.svg" />
    </a>
  </p>


  <p align="center">
    Utility for merging RNA-seq expression counts files from St. Jude Cloud. 
    <br />
    <br />
    <a href="https://github.com/stjudecloud/merge-counts/issues/new?assignees=&labels=&template=feature_request.md&title=Descriptive%20Title&labels=enhancement">Request Feature</a>
    ·
    <a href="https://github.com/stjudecloud/merge-counts/issues/new?assignees=&labels=&template=bug_report.md&title=Descriptive%20Title&labels=bug">Report Bug</a>
    ·
    ⭐ Consider starring the repo! ⭐
    <br />
  </p>
</p>

## 📚 Getting Started

### Installation

You can install `stjudecloud-merge-counts` using the Python Package Index ([PyPI](https://pypi.org/)).

```bash
pip install stjudecloud-merge-counts
```

### Usage

`stjudecloud-merge-counts` has 4 subcommands:
* `concordance-test` - Performs a `recursive` and `sequential` merge and verifies that the results are concordant.
* `metadata` - Compiles file metadata into a tab-delimited matrix.
* `recursive` - Merges count files in a recursive, divide-and-conquer strategy.
* `sequential` - Merges count files sequentially. This method requires significantly more time than the recursive approach.

All four subcommands require a set of DNAnexus file IDs to be supplied as commandline arguments.

For feature counts vended from St. Jude Cloud platform, the following example will merge the vended counts into a tab-delimited matrix. Replace `project-G2KfyQ09XB5BBKKf1BXx9ZkK` with the project identifier for your DNAnexus project containing feature counts.

```dx ls --brief project-G2KfyQ09XB5BBKKf1BXx9ZkK:/immediate/FEATURE_COUNTS/  | xargs stjudecloud-merge-counts recursive```

## Caveats

- When a file belongs to multiple datasets in St. Jude Cloud, we will pick a single dataset name to use for that file based on an ordered priority list. In short, we'll generally choose the _best_ dataset (from the St. Jude Cloud team's perspective). You can find the priority of datasets listed here: https://github.com/stjudecloud/merge-counts/blob/master/mergecounts/utils/dx.py#L19.
  - So, for example, if the file for SJABCD1234 belongs to both the PCGP and CSTN datasets, the sample name generated will include the PCGP dataset name (since it is higher priority), giving you the column name `SJABCD1234 (PCGP)`.


## 🖥️ Development

If you are interested in contributing to the code, please first review
our [CONTRIBUTING.md][contributing-md] document. 

To bootstrap a development environment, please use the following commands.

```bash
# Clone the repository
git clone git@github.com:stjudecloud/merge-counts.git
cd merge-counts

# Install the project using poetry
poetry install
```

## 🚧️ Tests

merge-counts provides a (currently patchy) set of tests — both unit and end-to-end.

```bash
py.test
```

## 🤝 Contributing

Contributions, issues and feature requests are welcome!<br />Feel free to check [issues page](https://github.com/stjudecloud/merge-counts/issues). You can also take a look at the [contributing guide][contributing-md].

## 📝 License

This project is licensed under the MIT License—see the [LICENSE.md][license-md] file for details.

Copyright © 2020 [St. Jude Cloud Team](https://github.com/stjudecloud).<br />

[contributing-md]: https://github.com/stjudecloud/merge-counts/blob/master/CONTRIBUTING.md
[license-md]: https://github.com/stjudecloud/merge-counts/blob/master/LICENSE.md
