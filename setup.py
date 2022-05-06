from setuptools import setup

setup(
    name='inv_war_twitter_proj',
    version='0.0.1',
    packages=['src', 'src.apps', 'src.common', 'src.mongodb', 'src.pub_sub', 'src.pub_sub.data_extract',
              'src.pub_sub.data_analytics', 'src.pub_sub.producer_call_twitter_api', 'src.twitter', 'src.analytics',
              'src.rest_flask', 'src.encryption_and_decryption_data', 'test', 'upgrades'],
    url='https://github.com/prasu22/inv_war_twitter_proj',
    license='',
    author='adityagupta',
    author_email='adityag1@sigmoidanalytics.com',
    description='setup.py '
)
