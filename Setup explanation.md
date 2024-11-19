# Infrastructure

We made an VM using AWS using Ubuntu 24.04 server.

VM has public IP, so public access is enabled.

Configuration is made via bootstrap script, then datasets are fed to VM via scp.

Next the datrasets are loaded into databases.

Testing script is uses to run and output result into file.
