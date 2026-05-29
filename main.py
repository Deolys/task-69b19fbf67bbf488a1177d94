import questionary
from langgraph.graph import StateGraph, START
from langgraph.constants import interrupt
from langgraph.types import Command
from langgraph.checkpoint.memory import InMemorySaver
from typing import TypedDict, List, Dict, Any

# 1. Define the state of the graph
class GraphState(TypedDict):
    # Initial data that might be needed by the node
    foo: str
    # Value to be filled by human input
    human_value: str | None

# 2. Node that triggers an interrupt
async def ask_human(state: GraphState) -> Dict[str, Any]:
    # Prepare interrupt payload
    payload = {
        "type": "confirm",
        "question": "Уверены, что хотите продолжить?",
        "allow_responds": ["approve", "reject"],
    }
    # Trigger the interrupt; execution pauses until resumed
    await interrupt(payload)
    # After resume, the payload will contain an 'answer' key
    answer = state.get("human_value")  # should be set by resume logic
    return {"node": {"human_value": answer}}

# 3. Build the graph with a checkpoint saver
builder = StateGraph(GraphState)
builder.add_node("ask", ask_human)
builder.set_entry_point(START, "ask")
checkpoint_saver = InMemorySaver()
graph = builder.compile(checkpointer=checkpoint_saver)

# 4. Run the graph and handle interrupts in a loop
async def main():
    # Unique thread id for this run
    thread_id = "demo-thread"
    config = {"configurable": {"thread_id": thread_id}}
    # Start streaming from initial state
    stream = graph.stream({"foo": "initial data", "human_value": None}, config)
    async for chunk in stream:
        if "__interrupt__" in chunk:
            # Extract the interrupt payload
            interrupt_payload = chunk["__interrupt__"][0].value
            # Show question to user and get answer
            answer = questionary.select(
                interrupt_payload["question"],
                choices=interrupt_payload["allow_responds"],
            ).ask()
            # Attach the answer back into the payload
            interrupt_payload["answer"] = answer
            # Resume graph with updated payload
            resume_stream = graph.stream(Command(resume=interrupt_payload), config)
            async for sub_chunk in resume_stream:
                if "node" in sub_chunk:
                    print("\nUpdated state after human input:", sub_chunk["node"])
        else:
            # Normal output from the node
            pass

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
