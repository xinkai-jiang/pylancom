from setuptools import setup

setup(
    name="pylancom",
    version="1.0.0",
    install_requires=["zmq", "colorama"],
    include_package_data=True,
    packages=["pylancom"],
)
