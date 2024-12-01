pg_ctl start
psql --command "ALTER USER postgres WITH ENCRYPTED PASSWORD 'postgres';"
python3 -u worker.py