def fix_indent(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    broken = """        if pass_through_accounts:
                    if pass_through_accounts:
            log_writer"""
    
    fixed = """        if pass_through_accounts:
            log_writer"""
            
    content = content.replace(broken, fixed)
    
    with open(file_path, "w") as f:
        f.write(content)

fix_indent("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
