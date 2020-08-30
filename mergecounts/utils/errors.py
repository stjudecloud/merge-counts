"""Error utilities for the merge-counts command line tool."""


def raise_error(
    message: str,
    suggest_report: bool = True,
    postlude: str = "Please report this error by filing a Github issue at https://github.com/stjudecloud/merge-counts/issues.",
) -> None:
    """Raise a RuntimeError and suggest they report the issue.

    Args:
        message (str): message to be displayed to the user.
        postlude (str, optional): message to be tacked onto the ends (Defaults to instructing the user to post issue to Github).
    """
    msg = message
    if suggest_report:
        msg = msg + " " + postlude
    raise RuntimeError(msg)
