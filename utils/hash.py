import hashlib


def generator_chash(comment: dict, _id: str) -> hashlib.md5:
    """
    댓글 hash 를 생성하여 반환한다
    다음 문자열을 합친 후 sha256 hash 값을 생성하여 반환한다
    """
    sha256 = hashlib.new('sha256')
    # mall 댓글인 경우 여러 몰에서 동일한 댓글이 있을 수 있으므로, 구매처를 합하여 해싱한다
    hash_str = f'{_id}' \
               f'{comment["publishedAtTimestamp"]}' \
               f'{comment["userName"]}' \
               f'{comment["review"]}' \
               f'{comment["contentText"][:10]}'

    sha256.update(hash_str.encode())

    return sha256.hexdigest()
