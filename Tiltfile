k8s_yaml('deploy/dev/manifests/ns.yaml')

k8s_yaml('deploy/dev/manifests/postgresql.yaml')
k8s_yaml('deploy/dev/manifests/redis.yaml')

k8s_resource('postgresql', port_forwards=5432)
k8s_resource('redis', port_forwards=6379)
