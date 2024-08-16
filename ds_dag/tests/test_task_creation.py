import unittest
from unittest import TestCase
# from ds_dag.dags.ds_dag_manager import DSDagManager


config = {
    "dags": {
        "test_dag": {
            "model": "test_model"
        }
    }
}


class TestTaskCreation(TestCase):

    def test_task_creation(self):
        pass
        # dag_manager = DSDagManager(
        #     config=config
        # )
        # ds_dag = dag_manager.create_dag('test_dag')
        # ds_dag.new_task_create_table()
        # self.assertEqual(2, len(ds_dag.dag.tasks))


if __name__ == '__main__':
    unittest.main()
