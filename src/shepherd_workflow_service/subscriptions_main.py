"""Compile subscriptions from the exact catalog deployed with the image.

Usage: python -m shepherd_workflow_service.subscriptions_main catalog.json bus-name
Outputs Terraform variable JSON; use -var-file on the Terraform deployment.
"""
import argparse
import hashlib
import json

from shepherd_workflow_service.adapters.real.workflow_catalog import JsonWorkflowCatalog


def compile_subscriptions(catalog, bus_name):
    selectors = set()
    for workflow in catalog.workflows:
        selectors.add((workflow.trigger.producer, workflow.trigger.event_type))
        for stage in workflow.stages:
            for selector in (stage.completion.success, stage.completion.failure):
                selectors.add((selector.producer, selector.event_type))
    # One rule per producer avoids a Cartesian product granting unintended combinations.
    producers = sorted({producer for producer, _ in selectors})
    return {
        "execution_subscriptions": {
            "producer-" + hashlib.sha256(producer.encode()).hexdigest()[:12]: {
                "source_bus_name": bus_name,
                "event_pattern": json.dumps({
                    "source": [producer],
                    "detail-type": sorted(t for p, t in selectors if p == producer),
                }),
            } for producer in producers
        }
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalog")
    parser.add_argument("bus_name")
    args = parser.parse_args()
    print(json.dumps(compile_subscriptions(
        JsonWorkflowCatalog(args.catalog).load(), args.bus_name
    ), indent=2))


if __name__ == "__main__":
    main()
