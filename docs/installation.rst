Installation
============

Server requirements::

    sudo apt-get install -y postgresql postgresql-server-dev-9.1
    sudo -u postgres createuser --superuser $USER
    createdb aque

    sudo vim /etc/postgresql/9.1/main/postgresql.conf
    # Add: listen_addresses = '10.0.0.221'

    sudo vim /etc/postgresql/9.1/main/pg_hba.conf
    # Add: host    aque    all     10.0.0.0/24     trust

    sudo service postgresql restart


Client requirements::

    sudo apt-get install -y libpq-dev

