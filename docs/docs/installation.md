# Installation

## Software Requirements

`dbworkload` requires at least Python 3.8 and the `pip` utility, installed and upgraded.

`dbworkload` dependencies are installed automatically by the `pip` tool.

It has run successfully on Ubuntu 20.04+, MacOSX on both Intel and Apple silicon.

## dbworkload installation

`dbworkload` comes already pre-packaged, [available from PyPI](https://pypi.org/project/dbworkload/).

Generally, you want to specify which of the [supported drivers](drivers.md) you want to install.

In below example, we install with the **Psycopg3** driver, so we run

```bash
# you must use pipx on latest Ubuntu
apt update
apt install -y python3-pip pipx

pip3 install --upgrade pip

pipx install dbworkload[postgres]
pipx ensurepath
```

Confirm installation is successful by running

```bash
dbworkload --version
```
