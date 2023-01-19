load('ext://restart_process', 'docker_build_with_restart')

default_registry('k3d-registry.localhost:5000')

local_resource(
    'go-compile',
    'make build'
    )

docker_build_with_restart(
       'prometheus-singlestore-adapter',
       '.',
       entrypoint=['/prometheus-postgresql-adapter'],
       dockerfile='Dockerfile',
       live_update=[sync('bin/', '/')]
)

k8s_yaml('./deployment.yaml')
k8s_resource('prometheus-singlestore-adapter', resource_deps=['go-compile'])
