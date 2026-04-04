from graphviz import Digraph


def check_deps_charts(models_cfg: dict, output_dir: str = "./output"):
    # Create directed graph
    dot = Digraph(format="png")
    dot.attr(rankdir="LR", size="8,5")

    added_edges = set()

    # Add edges: features → targets
    for model_name, m_cfg in models_cfg.items():
        for feature in m_cfg["features"]:
            for target in m_cfg["targets"]:
                edge = (feature, target)
                if edge not in added_edges:
                    dot.edge(feature, target, label="")
                    added_edges.add(edge)

    # Save and render
    # dot.render("model_flow", view=True)
    output_path = dot.render(f"{output_dir}/model_flow", format="png", cleanup=True)
    print(f"Flow chart saved to: {output_path}")


def obtain_all_tasks(tasks_cfg: dict, target_cfg: dict):

    results = {}

    try:
        all_tasks = tasks_cfg.split("->")
    except AttributeError:
        all_tasks = [tasks_cfg]

    for i, task in enumerate(all_tasks):
        task = task.strip()
        results[task] = target_cfg.get(task, [])

    return results
