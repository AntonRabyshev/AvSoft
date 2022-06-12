from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

retry_with_params = retry(retry=retry_if_exception_type(
    exception_types=(
        Exception
    )
),
    stop=(stop_after_delay(60)),
    wait=(wait_fixed(5))
)
