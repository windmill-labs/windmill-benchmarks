from prefect import flow

if __name__ == "__main__":
    flow.from_source(source="/opt/flows", entrypoint="unit.py:unit").deploy(
        name="bench", work_pool_name="bench"
    )
    print("deployed")
