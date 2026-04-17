import glob
import os

files = glob.glob("plans/gap/*_workstream_*.md")
results = {}

for f in files:
    with open(f, 'r') as fp:
        lines = fp.read().splitlines()
        
    in_questions = False
    questions = []
    for line in lines:
        if line.startswith("## Open Questions"):
            in_questions = True
            continue
        if in_questions:
            if line.startswith("## "): 
                in_questions = False
                continue
            if line.strip() != "":
                questions.append(line)
    
    if questions:
        base = os.path.basename(f)
        parts = base.split('_')
        if len(parts) >= 3:
            name = parts[0]
            ws = parts[2]
            key = f"Workstream {ws}"
            if key not in results:
                results[key] = []
            results[key].append(f"### From {name.capitalize()} ({base}):\n" + "\n".join(questions))

with open("plans/gap/open_questions_20260417.md", "w") as fp:
    fp.write("# Aggregated Open Questions\n\n")
    for ws in sorted(results.keys()):
        fp.write(f"## {ws}\n\n")
        for q in results[ws]:
            fp.write(q + "\n\n")
