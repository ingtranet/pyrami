import os

import pytest
pytestmark = pytest.mark.anyio

from twitter_stream.twitter import TwitterClient

@pytest.fixture
def twitter_client():
    bearer_token = os.environ.get('TWITTER_BEARER_TOKEN')
    if not bearer_token:
        pytest.skip('No environment variable TWITTER_BEARER_TOKEN')
    return TwitterClient(bearer_token)

async def test_twitter(twitter_client: TwitterClient):
    tweets = list()
    async with twitter_client.get_stream() as response:
        assert response.status_code == 200
        async for line in response.aiter_lines():
            tweets.append(line.strip())
            if len(tweets) >= 10:
                break