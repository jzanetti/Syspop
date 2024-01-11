import matplotlib.pyplot as plt

def validate_households(truth: dict, data: dict):
    # Create a figure and a set of subplots
    fig, ax = plt.subplots()

    # Plot data
    ax.bar(truth.keys(), truth.values(), width=0.4, label='True')
    ax.bar(data.keys(), data.values(), width=0.4, label='Syn population', align='edge')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_xlabel('Children number')
    ax.set_ylabel('Household number')
    ax.set_title('Comparison of census and synthetic population')
    ax.legend()

    plt.savefig("test.png",  bbox_inches='tight')
    plt.close()
