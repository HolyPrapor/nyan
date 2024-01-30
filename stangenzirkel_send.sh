mkdir -p data;

while :
do
    python3 -m nyan.send \
        --channels-info-path stangenzirkel_channels.json \
        --client-config-path configs/stangenzirkel_client_config.json \
        --mongo-config-path configs/mongo_config.json \
        --annotator-config-path configs/annotator_config.json \
        --renderer-config-path configs/stangenzirkel_renderer_config.json \
        --clusterer-config-path configs/stangenzirkel_clusterer_config.json \
        --ranker-config-path configs/stangenzirkel_ranker_config.json \
        --daemon-config-path configs/daemon_config.json;
done
