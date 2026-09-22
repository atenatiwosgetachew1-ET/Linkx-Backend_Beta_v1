def fix(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # In xvigilance_consumer.py, find FUND_FLOW and add pt=pass_through_accounts
    content = content.replace(
        "pass_through_accounts=pass_through_accounts)",
        "pass_through_accounts=pass_through_accounts, pt=pass_through_accounts)"
    )
    
    with open(file_path, "w") as f:
        f.write(content)

fix("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
print("Done")
