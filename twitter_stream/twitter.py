from contextlib import _AsyncGeneratorContextManager
import httpx


EXPENSIONS = [
    'attachments.poll_ids',
    'attachments.media_keys',
    'author_id',
    'entities.mentions.username',
    'geo.place_id',
    'in_reply_to_user_id',
    'referenced_tweets.id',
    'referenced_tweets.id.author_id'
]

MEDIA_FIELDS = [
    'duration_ms',
    'height',
    'media_key',
    'preview_image_url',
    'type',
    'url',
    'width',
    'public_metrics',
    'alt_text'
]

PLACE_FIELDS = [
    'contained_within',
    'country',
    'country_code',
    'full_name',
    'geo',
    'id',
    'name',
    'place_type'
]

POLL_FIELDS = [
    'duration_minutes',
    'end_datetime',
    'id',
    'options',
    'voting_status'
]

TWEET_FIELDS = [
    'attachments',
    'author_id',
    'context_annotations',
    'conversation_id',
    'created_at',
    'entities',
    'geo',
    'id',
    'in_reply_to_user_id',
    'lang',
    'public_metrics',
    'possibly_sensitive',
    'referenced_tweets',
    'reply_settings',
    'source',
    'text',
    'withheld'
]

USER_FIELDS = [
    'created_at',
    'description',
    'entities',
    'id',
    'location',
    'name',
    'pinned_tweet_id',
    'profile_image_url',
    'protected',
    'public_metrics',
    'url',
    'username',
    'verified',
    'withheld'
]

class TwitterClient():
    API_URL = 'https://api.twitter.com/2'

    def __init__(self, bearer_token) -> None:
        self.client = httpx.AsyncClient()
        self.bearer_token = bearer_token

    async def close(self) -> None:
        await self.client.aclose()

    def get_stream(self) -> _AsyncGeneratorContextManager[httpx.Response]:
        return self.client.stream(
                method='GET',
                timeout=20.0,
                url=self.API_URL + '/tweets/sample/stream',
                headers={'Authorization': f'Bearer {self.bearer_token}'},
                params={
                    'expansions': ','.join(EXPENSIONS),
                    'media.fields': ','.join(MEDIA_FIELDS),
                    'place.fields': ','.join(PLACE_FIELDS),
                    'poll.fields': ','.join(POLL_FIELDS),
                    'tweet.fields': ','.join(TWEET_FIELDS),
                    'user.fields': ','.join(USER_FIELDS)
                }
        )