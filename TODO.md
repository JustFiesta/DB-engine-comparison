# DB-engine-comparison

Comparison of the MongoDB and MariaDB engines by performance  

## TODO

1. Setup VM on Cloud/Docker with public access and spin two containers/servers (MariaDB + Mongo) - DONE

    Connection:

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

2. Import data from csvs - DONE

3. Setup explanation - DONE

4. compare usage of resources - ????

5. create excel

6. create paper
