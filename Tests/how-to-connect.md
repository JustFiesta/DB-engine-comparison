# Connections to DBs

  * MariaDB

      ```shell
      sudo mariadb --user=root --password=
      ```

  * MongoDB

      ```shell
      mongosh
      ```

  * Docker compose

      ```shell
      cd path/to/docker-compose
      docker compose up
      docker ps -a # look the state and id of continers
      docker exec -it bash id-of-container # connect to container
      ```
