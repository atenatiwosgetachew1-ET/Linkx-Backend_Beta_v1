def fix_consumer(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    old_block = """                                cur.execute("UPDATE xvigilance_checkpoints SET total_graph_analyzed = total_graph_analyzed + %s", (data.get('total_records', 0),))
                            conn.commit()"""
                            
    new_block = """                                cur.execute("UPDATE xvigilance_checkpoints SET total_graph_analyzed = total_graph_analyzed + %s", (data.get('total_records', 0),))
                                cur.execute("UPDATE xvigilance_slice_runs SET status = 'succeeded', finished_at = NOW() WHERE window_end = %s", (data.get('window_id'),))
                            conn.commit()"""
                            
    content = content.replace(old_block, new_block)
    
    with open(file_path, "w") as f:
        f.write(content)

fix_consumer("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
print("Done")
