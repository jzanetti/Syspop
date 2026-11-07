def check_deps_charts(models_cfg: dict, output_dir: str = "./output"):
    """Render dependency flowchart if graphviz is available."""
    try:
        from graphviz import Digraph
        from graphviz.backend.execute import ExecutableNotFound
    except ImportError:
        return

    dot = Digraph(format="png")
    dot.attr(rankdir="LR", size="8,5")

    added_edges = set()

    for model_name, m_cfg in models_cfg.items():
        for feature in m_cfg.get("features", []):
            for target in m_cfg.get("targets", {}):
                edge = (feature, target)
                if edge not in added_edges:
                    dot.edge(feature, target, label="")
                    added_edges.add(edge)

    try:
        output_path = dot.render(f"{output_dir}/model_flow", format="png", cleanup=True)
        print(f"Flow chart saved to: {output_path}")
    except Exception:
        pass


def obtain_all_tasks(tasks_cfg: dict, target_cfg: dict):
    results = {}

    try:
        all_tasks = tasks_cfg.split("->")
    except AttributeError:
        all_tasks = [tasks_cfg]

    for task in all_tasks:
        task = task.strip()
        results[task] = target_cfg.get(task, [])

    return results
