# compute-worker: asynchronous batch compute service for caifubao

from worker_app.bootstrap import prepare_app_tree

# Make the merged `app` package importable before any handler/model module is
# imported (worker_app.model imports `app.model.compute_task`).
prepare_app_tree()
