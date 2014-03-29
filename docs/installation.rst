Installation
============

Server requirements::

    sudo apt-get install -y postgresql postgresql-server-dev-9.1
    sudo -u postgres createuser --superuser $USER
    createdb aque
    # TODO: listen to the right interfaces
    # TODO: setup client authentication


Client requirements::

    sudo apt-get install -y libpq-dev

