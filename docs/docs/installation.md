# Installation

## Software Requirements

`dbworkload` requires at least Python 3.11 and the `pipx` utility.

`dbworkload` dependencies are installed automatically by the `pipx` tool.

## dbworkload installation

`dbworkload` comes already pre-packaged, [available from PyPI](https://pypi.org/project/dbworkload/).

Generally, you want to specify which of the [supported drivers](drivers.md) you want to install.

In below example, we install with the **Psycopg3** driver, so we run

```bash
# you must use pipx on latest Ubuntu
apt update
apt install -y pipx

pipx install dbworkload[postgres]
pipx ensurepath
```

Confirm installation is successful by running

```bash
dbworkload --version
```

## Build Python from source

If you are running an older version of Ubuntu, Python might be too old for running dbworkload.

In this case, you can build it from source, see below instructions.

Check current version - too old!

```bash
$ python3 -V
Python 3.10.12
````

Install build dependencies

```bash
sudo apt update

sudo apt install -y build-essential zlib1g-dev libncurses-dev libgdbm-dev libgdbm-compat-dev \
libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev libbz2-dev liblzma-dev \
uuid-dev libexpat1-dev tk-dev pkg-config make libzstd-dev wget ca-certificates
```

Get the link for the latest tarball from <https://www.python.org/downloads/source/>

```bash
wget https://www.python.org/ftp/python/3.14.6/Python-3.14.6.tar.xz

tar xvf Python-3.14.6.tar.xz 

cd Python-3.14.6
```

Finally, build it from source code

```bash
./configure --with-ensurepip=install --prefix=/usr/local/python3.14

make -j"$(nproc)"

sudo make altinstall
```

Once built, create the helpful symlinks

```bash
echo '/usr/local/python3.14/lib' | sudo tee /etc/ld.so.conf.d/python3.14.conf
sudo ldconfig
sudo ln -sf /usr/local/python3.14/bin/python3.14 /usr/local/bin/python3.14
```

Now it should all work! Test it out

```bash
$ python3.14 --version
Python 3.14.6

# validate by importing few important modules
python3.14 -c "import ssl, sqlite3, bz2, compression.zstd"
```

You can use `pip` module instead of standalone `pip`

```bash
python3.14 -m pip install dbworkload[postgres]
```

Now verify

```bash
$ which dbworkload
/home/ubuntu/.local/bin/dbworkload

$ dbworkload --version
dbworkload : 0.15.2
Python     : 3.14.6
```
