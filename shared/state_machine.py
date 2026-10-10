VALID_TRANSITIONS: dict[str, set[str]] = {
    "PENDING": {"RUNNING", "CANCELLED"},
    "RUNNING": {"COMPLETED", "FAILED", "RETRYING","PENDING"},
    "RETRYING": {"RUNNING", "CANCELLED","PENDING"},
    "CANCELLED": set(),  # terminal states
    "COMPLETED": set(),
    "FAILED": set(),
}


class InvalidTransition(Exception):
    pass


class ConcurrentModification(Exception):
    pass


def transition_job(
    job_id: str,
    expected_status: str,
    new_status: str,
    conn,
    **extra_fields,
) -> None:
    if new_status not in VALID_TRANSITIONS.get(expected_status, set()):
        raise InvalidTransition(
            f"Cannot transition job {job_id} from {expected_status} to {new_status}"
        )

    set_clauses = ["status = %s", "updated_at = now()"]
    values = [new_status]

    for field, value in extra_fields.items():
        set_clauses.append(f"{field} = %s")
        values.append(value)

    values.extend([job_id, expected_status])

    query = f"""
        UPDATE jobs
        SET {', '.join(set_clauses)}
        WHERE job_id = %s AND status = %s
    """

    result = conn.execute(query, values)

    if result.rowcount == 0:
        raise ConcurrentModification(
            f"Job {job_id} was not in status {expected_status} when transition was attempted "
            f"(already changed by another process, or does not exist)"
        )
