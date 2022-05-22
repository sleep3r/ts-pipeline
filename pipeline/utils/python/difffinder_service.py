from processor_service import *
import hashlib
from typing import Union


class DiffFinderService(ProcessorService):
    def _init_state(self):
        self.state.data_state = {}

    @staticmethod
    def get_full_hash(data: Union[dict, list]) -> str:
        def update_hash(data_to_hash):
            if isinstance(data_to_hash, dict):
                for keyword in sorted(data_to_hash.keys()):
                    # should i make there "update_hash(keyword)"? It could help to get another hash
                    # then keyword changed a bit like match -> matches
                    update_hash(data_to_hash[keyword])
            elif isinstance(data_to_hash, list):
                for item in data_to_hash:
                    update_hash(item)
            else:
                hash_code.update(str(data_to_hash).encode('utf-8'))

        hash_code = hashlib.sha256()
        update_hash(data)
        return hash_code.hexdigest()

    def _send_changes(self, data_id, data_to_send):
        if self.check_data_not_processed(data_id, data_to_send):
            self._send(json.dumps(data_to_send))
            print(f"{data_id} sent")

    def check_data_not_processed(self, data_id, data):
        if data_id not in self.state.data_state or self.state.data_state[data_id] != self.get_full_hash(data):
            self.state.data_state[data_id] = self.get_full_hash(data)
            return True
        # print(f"nothing new in {data_id}")  # TODO uncomment with debug mode logging when we finally invent logging
        return False