import re

def fix_indent(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # The broken block:
    #         if pass_through_accounts:
    #                     if pass_through_accounts:
    #             t_eff = time.time()
    
    # We replace the double if
    broken = """        if pass_through_accounts:
                    if pass_through_accounts:
            t_eff = time.time()"""
    
    fixed = """        if pass_through_accounts:
            t_eff = time.time()"""
            
    content = content.replace(broken, fixed)
    
    with open(file_path, "w") as f:
        f.write(content)

fix_indent("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
print("Done")
