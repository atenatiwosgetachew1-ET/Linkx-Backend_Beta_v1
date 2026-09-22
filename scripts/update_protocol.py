import re

def update_protocol(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    new_rule = """> 6. **NEVER provide copy-paste bash blocks that can trigger aggressive terminal line wrapping**:
>    Many SSH clients wrap text around 80 characters, causing line breaks on hyphens (`-`) or spaces. This shatters `cp` and `git` commands into invalid fragments (e.g., `cp: missing destination file operand`). Always provide ultra-short commands using sequential `cd` steps.

---"""

    content = content.replace("---\n\n## 4. Standard Server Update Templates", new_rule + "\n\n## 4. Standard Server Update Templates")
    
    with open(file_path, "w") as f:
        f.write(content)

update_protocol("/var/www/linkx-backend/docs/server_communication_and_update_protocol.md")
print("Done")
