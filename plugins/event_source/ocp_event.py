from openshift.dynamic import DynamicClient
from kubernetes import config, watch
from ansible_collections.ansible.eda.plugins.event_source import BaseEventSource
import json
import os


class EventSource(BaseEventSource):
    """
    Custom event source plugin to read OpenShift events with optional kubeconfig path.
    """

    def __init__(self, args):
        super().__init__(args)
        # Use the kubeconfig path from args, environment variable, or default
        self.kubeconfig_path = args.get("kubeconfig") or os.getenv("KUBECONFIG") or "~/.kube/config"
        self.namespace = args.get("namespace", "default")
        self.resource_type = args.get("resource_type", "events")  # Default to "events"

    def run(self, queue):
        """
        Connect to the OpenShift cluster and listen for events.
        """
        try:
            # Resolve kubeconfig path
            kubeconfig_path = os.path.expanduser(self.kubeconfig_path)

            # Load Kubernetes config
            k8s_client = config.new_client_from_config(config_file=kubeconfig_path)
            dyn_client = DynamicClient(k8s_client)

            # Watch the specified resource
            w = watch.Watch()
            resource = dyn_client.resources.get(api_version="v1", kind=self.resource_type)

            for event in w.stream(resource.list, namespace=self.namespace):
                event_data = {
                    "type": event["type"],
                    "object": event["object"].to_dict(),
                }
                self._log_debug(f"Event captured: {json.dumps(event_data, indent=2)}")

                # Add the event to the queue
                queue.put(event_data)

        except Exception as e:
            self._log_error(f"Error reading OpenShift events: {str(e)}")

    def _log_debug(self, message):
        """
        Log debug messages to the console or a file.
        """
        self.log.debug(f"[OpenShift Event Plugin] {message}")

    def _log_error(self, message):
        """
        Log error messages to the console or a file.
        """
        self.log.error(f"[OpenShift Event Plugin] {message}")

