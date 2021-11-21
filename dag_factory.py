from airflow import DAG

class AirflowDAGFactory:
    """Factory method for getting a DAG."""

    @staticmethod
    def airflow_dag(*keys: List[Any], **kwargs: Dict[AnyStr, Any]) -> DAG:
        """
        Factory method for getting a DAG.
        Been written to avoid repeating ourselves in the common patrs.
        """
        return DAG(
            *keys,
            dagrun_timeout=kwargs.pop("dagrun_timeout", timedelta(hours=DAG_RUN_DEFAULT_TIMEOUT_HOURS)),
            **kwargs,
        )
