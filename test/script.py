from pathlib import Path

current_dir= Path.cwd()
current_file=Path(__file__).name #gives file name

print(f"Files in {current_dir}")


for file_path in current_dir.iterdir():
    if file_path.name == current_file:
        continue
    print(f"    - {file_path.name}")

    if file_path.is_file():
        content=file_path.read_text(encoding='utf-8')
        print(f"        Content : {content}")

