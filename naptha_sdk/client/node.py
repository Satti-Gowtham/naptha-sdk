from datetime import datetime
from httpx import HTTPStatusError, RemoteProtocolError
from naptha_sdk.schemas import AgentRun, AgentRunInput, OrchestratorRun, OrchestratorRunInput
from naptha_sdk.utils import get_logger
from pathlib import Path
from typing import Dict, Optional, Any, List, Tuple, Union
import httpx
import json
import os
import shutil
import tempfile
import time
import traceback
import uuid
import websockets
import zipfile
import grpc 
from naptha_sdk.client import grpc_server_pb2_grpc
from naptha_sdk.client import grpc_server_pb2
from google.protobuf import struct_pb2

logger = get_logger(__name__)
HTTP_TIMEOUT = 300

class Node:
    def __init__(self, node_url: Optional[str] = None, indirect_node_id: Optional[str] = None, routing_url: Optional[str] = None):
        self.node_url = node_url
        self.indirect_node_id = indirect_node_id
        self.routing_url = routing_url
        if self.node_url.startswith('ws://'):
            self.server_type = 'ws'
        elif self.node_url.startswith('http://'):
            self.server_type = 'http'
        else:
            self.server_type = 'grpc'
        self.connections = {}

        # at least one of node_url and indirect_node_id must be set
        if not node_url and not indirect_node_id:
            raise ValueError("Either node_url or indirect_node_id must be set")
        
        # if indirect_node_id is set, we need the routing_url to be set
        if indirect_node_id and not routing_url:
            raise ValueError("routing_url must be set if indirect_node_id is set")
        
        self.access_token = None
        logger.info(f"Node URL: {node_url}")

    async def connect_ws(self, action: str):
        client_id = str(uuid.uuid4())
        full_url = f"{self.node_url}/ws/{action}/{client_id}"
        logger.info(f"Connecting to WebSocket: {full_url}")
        ws = await websockets.connect(full_url)
        self.connections[client_id] = ws
        self.current_client_id = client_id
        return client_id

    async def disconnect_ws(self, client_id: str):
        if client_id in self.connections:
            await self.connections[client_id].close()
            del self.connections[client_id]
        if self.current_client_id == client_id:
            self.current_client_id = None

    async def send_receive_ws(self, data, action: str):
        client_id = await self.connect_ws(action)
        
        try:
            message = json.dumps(data)
            await self.connections[client_id].send(message)
            
            response = await self.connections[client_id].recv()
            return json.loads(response)
        finally:
            await self.disconnect_ws(client_id)

    async def check_user(self, user_input):
        print("Checking user... ", user_input)
        if self.server_type == 'http':
            return await self.check_user_http(user_input)
        elif self.server_type == 'ws':
            return await self.check_user_ws(user_input)
        elif self.server_type == 'grpc':
            return await self.check_user_grpc(user_input)
        else:
            raise ValueError("Invalid server type")

    async def register_user(self, user_input):
        if self.server_type == 'http':
            result = await self.register_user_http(user_input)
        elif self.server_type == 'ws':
            result = await self.register_user_ws(user_input)
        elif self.server_type == 'grpc':
            result = await self.register_user_grpc(user_input)
        else:
            raise ValueError("Invalid server type")
        
        if result is None:
            raise ValueError("User registration failed: returned None")
        
        return result

    async def run_agent(self, agent_run_input: AgentRunInput) -> AgentRun:
        if self.server_type == 'http':
            result = await self.run_agent_and_poll(agent_run_input)
        elif self.server_type == 'ws':
            result = await self.run_agent_ws(agent_run_input)
        elif self.server_type == 'grpc':
            result = await self.run_agent_grpc(agent_run_input)
        else:
            raise ValueError("Invalid server type")
        
        if result is None:
            raise ValueError("run_agent returned None")
        
        return result

    async def run_agent_and_poll(self, agent_run_input: AgentRunInput) -> AgentRun:
        assert self.server_type == 'http', "run_agent_and_poll should only be called for HTTP server type"
        agent_run = await self.run_agent_http(agent_run_input)
        print(f"Agent run started: {agent_run}")

        current_results_len = 0
        while True:
            agent_run = await self.check_agent_run(agent_run)
            output = f"{agent_run.status} {agent_run.agent_deployment.module['type']} {agent_run.agent_deployment.module['name']}"
            print(output)

            if len(agent_run.results) > current_results_len:
                print("Output: ", agent_run.results[-1])
                current_results_len += 1

            if agent_run.status == 'completed':
                break
            if agent_run.status == 'error':
                break

            time.sleep(3)

        if agent_run.status == 'completed':
            print(agent_run.results)
        else:
            print(agent_run.error_message)
        return agent_run

    async def run_orchestrator(self, orchestrator_run_input: OrchestratorRunInput) -> OrchestratorRun:
        if self.server_type == 'http':
            result = await self.run_orchestrator_and_poll(orchestrator_run_input)
        elif self.server_type == 'ws':
            result = await self.run_orchestrator_ws(orchestrator_run_input)
        else:
            raise ValueError("Invalid server type")
        
        if result is None:
            raise ValueError("run_orchestrator returned None")
        
        return result

    async def run_orchestrator_and_poll(self, orchestrator_run_input: OrchestratorRunInput) -> OrchestratorRun:
        assert self.server_type == 'http', "run_orchestrator_and_poll should only be called for HTTP server type"
        orchestrator_run = await self.run_orchestrator_http(orchestrator_run_input)
        print(f"Orchestrator run started: {orchestrator_run}")

        current_results_len = 0
        while True:
            orchestrator_run = await self.check_orchestrator_run(orchestrator_run)
            output = f"{orchestrator_run.status} {orchestrator_run.orchestrator_deployment.module['type']} {orchestrator_run.orchestrator_deployment.module['name']}"
            print(output)
            if len(orchestrator_run.results) > current_results_len:
                print("Output: ", orchestrator_run.results[-1])
                current_results_len += 1

            if orchestrator_run.status == 'completed':
                break
            if orchestrator_run.status == 'error':
                break

            time.sleep(3)

        if orchestrator_run.status == 'completed':
            print(orchestrator_run.results)
        else:
            print(orchestrator_run.error_message)
        return orchestrator_run

    async def create_agent_run(self, agent_run_input: AgentRunInput) -> AgentRun:
        assert self.server_type == 'http', "create_agent_run should only be called for HTTP server type"
        logger.info(f"Creating agent run with input: {agent_run_input}")
        logger.info(f"Node URL: {self.node_url}")
        return await self.create_agent_run_http(agent_run_input)

    async def check_agent_run(self, agent_run: AgentRun) -> AgentRun:
        assert self.server_type == 'http', "check_agent_run should only be called for HTTP server type"
        return await self.check_agent_run_http(agent_run)

    async def check_orchestrator_run(self, orchestrator_run: OrchestratorRun) -> OrchestratorRun:
        assert self.server_type == 'http', "check_orchestrator_run should only be called for HTTP server type"
        return await self.check_orchestrator_run_http(orchestrator_run)

    async def update_agent_run(self, agent_run: AgentRun):
        assert self.server_type == 'http', "check_agent_run should only be called for HTTP server type"
        return await self.update_agent_run_http(agent_run)

    async def read_storage(self, agent_run_id, output_dir, ipfs=False):
        assert self.server_type == 'http', "read_storage should only be called for HTTP server type"
        return await self.read_storage_http(agent_run_id, output_dir, ipfs)

    async def write_storage(self, storage_input: str, ipfs: bool = False, publish_to_ipns: bool = False, update_ipns_name: Optional[str] = None):
        assert self.server_type == 'http', "write_storage should only be called for HTTP server type"
        return await self.write_storage_http(storage_input, ipfs, publish_to_ipns, update_ipns_name)

    async def check_user_http(self, user_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if a user exists on a node
        """
        endpoint = self.node_url + "/user/check"
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                headers = {
                    'Content-Type': 'application/json', 
                }
                response = await client.post(
                    endpoint, 
                    json=user_input,
                    headers=headers
                )
                response.raise_for_status()
            return json.loads(response.text)
        except HTTPStatusError as e:
            logger.info(f"HTTP error occurred: {e}")
            raise  
        except RemoteProtocolError as e:
            error_msg = f"Check user failed to connect to the server at {self.node_url}. Please check if the server URL is correct and the server is running. Error details: {str(e)}"
            logger.info(error_msg)
            raise 
        except Exception as e:
            logger.info(f"An unexpected error occurred: {e}")
            raise

    async def check_user_ws(self, user_input: Dict[str, str]):
        response = await self.send_receive_ws(user_input, "check_user")
        logger.info(f"Check user response: {response}")
        return response

    async def check_user_grpc(self, user_input: Dict[str, str]):
        async with grpc.aio.insecure_channel(self.node_url) as channel:
            stub = grpc_server_pb2_grpc.GrpcServerStub(channel)
            request = grpc_server_pb2.CheckUserRequest(
                user_id=user_input.get('user_id', ''),
                public_key=user_input.get('public_key', '')
            )
            response = await stub.CheckUser(request)
            return {
                'is_registered': response.is_registered,
                'user_id': response.user_id
            }

    async def register_user_grpc(self, user_input: Dict[str, str]):
        async with grpc.aio.insecure_channel(self.node_url) as channel:
            stub = grpc_server_pb2_grpc.GrpcServerStub(channel)
            request = grpc_server_pb2.RegisterUserRequest(
                public_key=user_input.get('public_key', '')
            )
            response = await stub.RegisterUser(request)
            return {
                'id': response.id,
                'public_key': response.public_key,
                'created_at': response.created_at
            }

    async def run_agent_grpc(self, agent_run_input: AgentRunInput):
        async with grpc.aio.insecure_channel(self.node_url) as channel:
            stub = grpc_server_pb2_grpc.GrpcServerStub(channel)
            
            # Convert input data to Struct
            input_struct = struct_pb2.Struct()
            if agent_run_input.inputs:
                if isinstance(agent_run_input.inputs, dict):
                    input_data = agent_run_input.inputs.dict() if hasattr(agent_run_input.inputs, 'dict') else agent_run_input.inputs
                    input_struct.update(input_data)
            
            # Create agent module and deployment
            agent_module = grpc_server_pb2.AgentModule(
                name=agent_run_input.agent_deployment.module['name']
            )
            
            agent_deployment = grpc_server_pb2.AgentDeployment(
                name=agent_run_input.agent_deployment.name,
                module=agent_module,
                worker_node_url=agent_run_input.agent_deployment.worker_node_url
            )
            
            # Create request
            request = grpc_server_pb2.AgentRunInput(
                consumer_id=agent_run_input.consumer_id,
                agent_deployment=agent_deployment,
                input_struct=input_struct
            )
            
            final_response = None
            async for response in stub.RunAgent(request):
                final_response = response
                logger.info(f"Got response: {final_response}")
                
            return AgentRun(
                consumer_id=agent_run_input.consumer_id,
                inputs=agent_run_input.inputs,
                agent_deployment=agent_run_input.agent_deployment,
                orchestrator_runs=[],
                status=final_response.status,
                error=final_response.status == "error",
                id=final_response.id,
                results=list(final_response.results),
                error_message=final_response.error_message,
                created_time=final_response.created_time,
                start_processing_time=final_response.start_processing_time,
                completed_time=final_response.completed_time,
                duration=final_response.duration,
                input_schema_ipfs_hash=final_response.input_schema_ipfs_hash
            )

    async def register_user_http(self, user_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Register a user on a node
        """
        endpoint = self.node_url + "/user/register"
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                headers = {
                    'Content-Type': 'application/json', 
                }
                response = await client.post(
                    endpoint, 
                    json=user_input,
                    headers=headers
                )
                response.raise_for_status()
            return json.loads(response.text)
        except HTTPStatusError as e:
            logger.info(f"HTTP error occurred: {e}")
            raise  
        except RemoteProtocolError as e:
            error_msg = f"Register user failed to connect to the server at {self.node_url}. Please check if the server URL is correct and the server is running. Error details: {str(e)}"
            logger.error(error_msg)
            raise 
        except Exception as e:
            logger.info(f"An unexpected error occurred: {e}")
            raise

    async def register_user_ws(self, user_input: Dict[str, str]):
        response = await self.send_receive_ws(user_input, "register_user")
        logger.info(f"Register user response: {response}")
        return response

    async def _run_http(self, run_input: Union[AgentRunInput, OrchestratorRunInput], run_type: str) -> Union[AgentRun, OrchestratorRun]:
        """
        Generic method to run either an agent or orchestrator on a node
        
        Args:
            run_input: Either AgentRunInput or OrchestratorRunInput
            run_type: Either 'agent' or 'orchestrator'
        """
        print(f"Running {run_type}...")
        print(f"Run input: {run_input}")
        print(f"Node URL: {self.node_url}")

        endpoint = f"{self.node_url}/{run_type}/run"
        
        # Convert dict to appropriate input type if needed
        input_class = AgentRunInput if run_type == 'agent' else OrchestratorRunInput
        if isinstance(run_input, dict):
            run_input = input_class(**run_input)

        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {self.access_token}',
                }
                response = await client.post(
                    endpoint,
                    json=run_input.model_dump(),
                    headers=headers
                )
                response.raise_for_status()
                
                # Convert response to appropriate return type
                return_class = AgentRun if run_type == 'agent' else OrchestratorRun
                return return_class(**json.loads(response.text))
        except HTTPStatusError as e:
            logger.info(f"HTTP error occurred: {e}")
            raise
        except RemoteProtocolError as e:
            error_msg = f"Run {run_type} failed to connect to the server at {self.node_url}. Please check if the server URL is correct and the server is running. Error details: {str(e)}"
            logger.error(error_msg)
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

    async def run_agent_http(self, agent_run_input: AgentRunInput) -> AgentRun:
        """Run an agent on a node"""
        return await self._run_http(agent_run_input, 'agent')

    async def run_orchestrator_http(self, orchestrator_run_input: OrchestratorRunInput) -> OrchestratorRun:
        """Run an orchestrator on a node"""
        return await self._run_http(orchestrator_run_input, 'orchestrator')
    async def run_agent_ws(self, agent_run_input: AgentRunInput) -> AgentRun:
        response = await self.send_receive_ws(agent_run_input, "run_agent")
        
        if response['status'] == 'success':
            return AgentRun(**response['data'])
        else:
            logger.error(f"Error running agent: {response['message']}")
            raise Exception(response['message'])

    async def check_agent_run_http(self, agent_run: AgentRun) -> AgentRun:
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                response = await client.post(
                    f"{self.node_url}/agent/check", json=agent_run.model_dump()
                )
                response.raise_for_status()
            return AgentRun(**json.loads(response.text))
        except HTTPStatusError as e:
            logger.info(f"HTTP error occurred: {e}")
            raise  
        except Exception as e:
            logger.info(f"An unexpected error occurred: {e}")
            logger.info(f"Full traceback: {traceback.format_exc()}")

    async def check_orchestrator_run_http(self, orchestrator_run: OrchestratorRun) -> OrchestratorRun:
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                response = await client.post(
                    f"{self.node_url}/orchestrator/check", json=orchestrator_run.model_dump()
                )
                response.raise_for_status()
            return OrchestratorRun(**json.loads(response.text))
        except HTTPStatusError as e:
            logger.info(f"HTTP error occurred: {e}")
            raise  
        except Exception as e:
            logger.info(f"An unexpected error occurred: {e}")
            logger.info(f"Full traceback: {traceback.format_exc()}")

    async def create_agent_run_http(self, agent_run_input: AgentRunInput) -> AgentRun:
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                response = await client.post(
                    f"{self.node_url}/monitor/create_agent_run", json=agent_run_input.model_dump()
                )
                response.raise_for_status()
            return AgentRun(**json.loads(response.text))
        except HTTPStatusError as e:
            logger.info(f"HTTP error occurred: {e}")
            raise  
        except Exception as e:
            logger.info(f"An unexpected error occurred: {e}")
            logger.info(f"Full traceback: {traceback.format_exc()}")

    async def update_agent_run_http(self, agent_run: AgentRun):
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                response = await client.post(
                    f"{self.node_url}/monitor/update_agent_run", json=agent_run.model_dump()
                )
                response.raise_for_status()
            return AgentRun(**json.loads(response.text))
        except HTTPStatusError as e:
            logger.info(f"HTTP error occurred: {e}")
            raise  
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            error_details = traceback.format_exc()
            print(f"Full traceback: {error_details}")

    async def read_storage_http(self, agent_run_id: str, output_dir: str, ipfs: bool = False) -> str:
        print("Reading from storage...")
        try:
            endpoint = f"{self.node_url}/{'storage/read_ipfs' if ipfs else 'storage/read'}/{agent_run_id}"

            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                response = await client.get(endpoint)
                response.raise_for_status()
                storage = response.content  
                print("Retrieved storage.")
            
                # Temporary file handling
                temp_file_name = None
                with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmp_file:
                    tmp_file.write(storage)  # storage is a bytes-like object
                    temp_file_name = tmp_file.name
        
                # Ensure output directory exists
                output_path = Path(output_dir)
                output_path.mkdir(parents=True, exist_ok=True)
        
                # Check if the file is a zip file and extract if true
                if zipfile.is_zipfile(temp_file_name):
                    with zipfile.ZipFile(temp_file_name, 'r') as zip_ref:
                        zip_ref.extractall(output_path)
                    print(f"Extracted storage to {output_dir}.")
                else:
                    shutil.copy(temp_file_name, output_path)
                    print(f"Copied storage to {output_dir}.")

                # Cleanup temporary file
                Path(temp_file_name).unlink(missing_ok=True)
        
                return output_dir         
        except HTTPStatusError as e:
            logger.info(f"HTTP error occurred: {e}")
            raise  
        except Exception as e:
            logger.info(f"An unexpected error occurred: {e}")
            logger.info(f"Full traceback: {traceback.format_exc()}")

    async def write_storage_http(self, storage_input: str, ipfs: bool = False, publish_to_ipns: bool = False, update_ipns_name: str = None) -> Dict[str, Any]:
        """Write storage to the node."""
        print("Writing storage")
        try:
            file = prepare_files(storage_input)
            endpoint = f"{self.node_url}/storage/write_ipfs" if ipfs else f"{self.node_url}/storage/write"
            
            if update_ipns_name:
                publish_to_ipns = True

            data = {
                "publish_to_ipns": publish_to_ipns,
                "update_ipns_name": update_ipns_name
            }
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                response = await client.post(
                    endpoint, 
                    files=file,
                    data=data,
                    timeout=600
                )
                response.raise_for_status()
                return response.json()
        except HTTPStatusError as e:
            logger.info(f"HTTP error occurred: {e}")
            raise  
        except Exception as e:
            logger.info(f"An unexpected error occurred: {e}")
            logger.info(f"Full traceback: {traceback.format_exc()}")
            return {}
        
def zip_directory(file_path, zip_path):
    """Utility function to zip the content of a directory while preserving the folder structure."""
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(file_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, start=os.path.abspath(file_path).split(os.sep)[0])
                zipf.write(file_path, arcname)

def prepare_files(file_path: str) -> List[Tuple[str, str]]:
    """Prepare files for upload."""
    if os.path.isdir(file_path):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmpfile:
            zip_directory(file_path, tmpfile.name)
            tmpfile.close()  
            file = {'file': open(tmpfile.name, 'rb')}
    else:
        file = {'file': open(file_path, 'rb')}
    
    return file

