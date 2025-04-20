# from https://github.com/YADRO-KNS/py-lspci.git
import re
import typing

# import fabric
# import invoke

from pci_parser import PCIParser, PCIDevice
from run_command import run_remote_shell_cmd


class DoesNotExist(Exception):
    def __init__(self, **kwargs):
        self.arguments = kwargs

    def __str__(self):
        return f"Unable to find PCI Device matching: {str(self.arguments)}"


class PCISelect(object):
    def __init__(self, devices: typing.List[PCIDevice]):
        self._devices = devices
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._devices):
            self._index = 0
            raise StopIteration
        else:
            self._index += 1
            return self._devices[(self._index - 1)]

    def count(self) -> int:
        return len(self._devices)

    def select(self, **kwargs) -> "PCISelect":
        if len(kwargs.items()) != 0:
            return PCISelect(devices=[d for d in self._devices if d.match(**kwargs)])
        else:
            return PCISelect(devices=self._devices)

    def get(self, **kwargs) -> PCIDevice:
        result = self.select(**kwargs)
        if result.count() != 0:
            return next(result)
        else:
            raise DoesNotExist(**kwargs)


# noinspection PyBroadException
class ScannerPCI(object):
    def __init__(
        self,
        ip: str,
        username: str = None,
        password: str = None,
        port: int = 22,
        logfile: typing.TextIO = None,
        timeout: int = 10,
    ):
        self.ip = ip
        self.username = username
        self.password = password
        self.port = port
        self.logfile = logfile
        self.timeout = timeout

        self._parser: typing.Optional[PCIParser] = None
        # self._console: typing.Optional[fabric.Connection] = None

    # def _get_console(self) -> fabric.Connection:
    #     if self._console is None:
    #         connect_kwargs = {}
    #         if self.password:
    #             connect_kwargs['password'] = self.password
    #         self._console = fabric.Connection(
    #             host=self.ip,
    #             user=self.username,
    #             port=self.port,
    #             config=fabric.Config(),
    #             connect_timeout=self.timeout,
    #             connect_kwargs=connect_kwargs,
    #         )
    #     return self._console

    def _run_command(self, command: str) -> typing.List[str]:
        pattern = r"\[sudo\].*:"
        # sudo_watcher = invoke.Responder(pattern=pattern, response=f"{self.password}\n")

        if self.username != "root":
            command = f"sudo {command}"

        if self.logfile is not None:
            self.logfile.write(f"{self.username}@{self.ip}:~$ {command}")

        # if self.ip == "127.0.0.1":
        #     result: fabric.Result = self._get_console().local(command, pty=True, hide="both", watchers=[sudo_watcher])
        # else:
        #     result: fabric.Result = self._get_console().run(command, pty=True, hide="both", watchers=[sudo_watcher])
        ret= run_remote_shell_cmd(f"{self.username}@{self.ip}", command)

        output = [line for line in ret.stdout.splitlines() if not bool(re.match(pattern, line))]

        if self.logfile is not None:
            for line in output:
                self.logfile.write(line)

            self.logfile.write(f"{self.username}@{self.ip}:~$ echo $?")
            self.logfile.write(f"{self.username}@{self.ip}:~$ {ret.returncode}")

        return output

    def _get_pci_addresses(self) -> typing.List[str]:
        """
        Get all PCI addresses from target machine device
        :return: List of PCI addresses
        """
        result: typing.List[str] = []
        try:
            command = "lspci -D | grep -o -e '[0-9A-Za-z]\{4\}:[0-9A-Za-z]\{2\}:[0-9A-Za-z]\{2\}.[0-9A-Za-z]\{1\}' | sed $'s/[^[:print:]]//g'"
            reply = self._run_command(command=command)
            for line in reply:
                if bool(re.match(r"""[0-9a-fA-F]{4}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}.[0-9a-fA-F]""", line)):
                    result.append(line)
        except Exception as error:
            print(error)
        finally:
            return result

    def _scan_single_pci_device(self, address: str) -> typing.Optional[typing.List[str]]:
        """
        Scan PCI device
        :param address: Address target to scan
        :return: PCI device data or Nothing
        """
        result = None
        try:
            result = self._run_command(command=f"lspci -s {address} -D -vv")
        except Exception as error:
            print(error)
        finally:
            return result

    def _get_pci(self, force_rescan: bool = False) -> typing.List[PCIDevice]:
        if self._parser is None or force_rescan is True:
            # self._get_console().open()
            self._parser = PCIParser(
                data=[self._scan_single_pci_device(address=address) for address in self._get_pci_addresses()]
            )
        return self._parser.devices

    def pci_rescan(self) -> None:
        self._run_command(command="sh -c 'echo 1 > /sys/bus/pci/rescan'")

    def select(self, force_rescan: bool = False, **kwargs) -> PCISelect:
        return PCISelect(devices=self._get_pci(force_rescan=force_rescan)).select(**kwargs)

    def get(self, force_rescan: bool = False, **kwargs) -> PCIDevice:
        return self.select(force_rescan=force_rescan).get(**kwargs)

    def get_connected(self, parent: PCIDevice, force_rescan: bool = False) -> PCISelect:
        if parent.is_host_bridge:
            return PCISelect(
                devices=[
                    d
                    for d in self.select(force_rescan=force_rescan, addr_dom=parent.addr_dom, addr_bus=parent.addr_bus)
                    if d != parent
                ]
            )
        elif parent.is_root_port or parent.is_downstream:
            return PCISelect(
                devices=[
                    d
                    for d in self.select(
                        force_rescan=force_rescan, addr_dom=parent.addr_dom, addr_bus=parent.pci_bus.secondary
                    )
                ]
            )
        elif parent.is_upstream:
            return PCISelect(
                devices=[
                    d
                    for d in self.select(force_rescan=force_rescan, addr_dom=parent.addr_dom, is_downstream=True)
                    if d.pci_bus.primary == parent.pci_bus.secondary
                ]
            )
        else:
            return PCISelect(devices=[])

    def get_all_connected_devices(self, parent: PCIDevice, force_rescan: bool = False) -> typing.List[PCIDevice]:
        connected = [parent]
        for connected_device in self.get_connected(parent=parent, force_rescan=force_rescan):
            connected.extend(self.get_all_connected_devices(connected_device))
        return connected
