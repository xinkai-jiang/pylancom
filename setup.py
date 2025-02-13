from setuptools import setup

setup(
    name="pylancom",
    version="0.1.1",
    install_requires=["zmq", "colorama"],
    include_package_data=True,
    packages=["pylancom"],
)
