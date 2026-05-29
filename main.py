import questionary
from langgraph.graph import StateGraph, START
from langgraph.constants import interrupt, Command
from langgraph.checkpoint.memory import InMemorySaver
from typing import TypedDict, List, Dict

class GraphState(TypedDict):
    human_value: str | None
    foo: str

# Node that triggers an interrupt and waits for user input
async def ask_user(state: GraphState) -> GraphState:
    # Trigger interrupt with structured payload
    payload = {
        "type": "confirm",
        "question": "Уверены, что хотите продолжить?",
        "allow_responds": ["approve", "reject"],
    }
    # The node will pause until resumed
    await interrupt(payload)
    # After resume, the payload may contain an answer field
    if isinstance(state.get("human_value"), dict) and "answer" in state["human_value"]:
        answer = state["human_value"]["answer"]
    else:
        answer = None
    return {"human_value": answer, "foo": state.get("foo", "")}

# Build the graph
builder = StateGraph(GraphState)
builder.add_node("ask_user", ask_user)
builder.set_entry_point(START)
builder.add_edge(START, "ask_user")
# No further edges; graph ends after node
graph = builder.compile(checkpointer=InMemorySaver())

# Run the graph with interrupt handling loop
async def main():
    from langchain_core.messages import BaseMessage
    # Initial state
    init_state: GraphState = {"human_value": None, "foo": "initial data"}
    thread_id = "demo-thread"
    config = {"configurable": {"thread_id": thread_id}}
    stream = graph.stream(init_state, config)
    async for chunk in stream:
        # Check if interrupt payload is present
        if "__interrupt__" in chunk:
            interrupt_payloads = chunk["__interrupt__"]
            # Take the first interrupt (there should be only one)
            intr = interrupt_payloads[0]
            payload = intr.value  # dict passed to interrupt()
            print("\n!!! interrupt received !!!")
            print(payload)
            # Ask user via questionary
            answer = questionary.select(
                message=payload.get("question", "Choose an option:"),
                choices=payload.get("allow_responds", []),
            ).ask()
            # Attach answer to payload and resume
            payload["answer"] = answer
            await stream.send(Command(resume=payload))
        else:
            # Normal output; print state updates
            if "node" in chunk:
                print("\nNode result:", chunk["node"])  
            if "state" in chunk:
                print("State:", chunk["state"])  
    print("\nGraph execution finished.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
