from prefect import flow
from prefect.blocks.system import Secret


@flow
def add_secret_block():
    Secret(value="passwordexample").save(name="mysecret")


if __name__ == "__main__":
    add_secret_block()
    secret_block = Secret.load("mysecret")
    # Access the stored secret
    secret_block.get()
