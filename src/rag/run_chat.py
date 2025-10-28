from src.rag.rag_pipeline import run_rag

while True:
    query = input("\n💬 Ask The Guardian bot: ")
    if query.lower() in {"exit", "quit"}:
        break
    response = run_rag(query)
    print("\n🧠 Answer:", response["answer"])
    print("\n📚 Sources:")
    for s in response["sources"]:
        print("•", s.get("title", "N/A"), "|", s.get("section", ""), "| score:", s.get("score"))