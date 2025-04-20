# from https://github.com/YADRO-KNS/py-lspci.git
import typing


class PCIBus(object):
    def __init__(self, data: str):
        self._data = data
        self.primary: typing.Optional[str] = None
        self.secondary: typing.Optional[str] = None
        self.subordinate: typing.Optional[str] = None
        self.sec_latency: typing.Optional[str] = None

        for entry in self._data.split(","):
            name = entry.split("=")[0].strip()
            if name == "primary":
                self.primary = entry.split("=")[1].strip().upper()
            if name == "secondary":
                self.secondary = entry.split("=")[1].strip().upper()
            if name == "subordinate":
                self.subordinate = entry.split("=")[1].strip().upper()
            if name == "sec-latency":
                self.sec_latency = entry.split("=")[1].strip().upper()


class PCILink(object):
    """
    Class placeholder for PCI device link parameters.
    """

    def __init__(self, lnkcap: str, lnksta: str) -> None:
        self.port = int(lnkcap.split(",")[0].split("#")[1])
        self.max_speed = lnkcap.split(",")[1].split(" ")[2]
        self.max_width = lnkcap.split(",")[2].split(" ")[2]

        self.speed = lnksta.split(",")[0].split(" ")[1]
        self.width = lnksta.split(",")[1].split(" ")[2]

    def __str__(self) -> str:
        return f"[{self.width}/{self.max_width}][{self.speed}/{self.max_speed}]"

    def __eq__(self, other) -> bool:
        if type(other) == PCILink:
            return str(self) == str(other)
        else:
            return False

    def __ne__(self, other) -> bool:
        if type(other) == PCILink:
            return str(self) != str(other)
        else:
            return True


class PCIAddress(object):
    def __init__(self, address: str):
        self.address = address
        self.dom = self.address.split(":")[0].upper()
        self.bus = self.address.split(":")[1].upper()
        self.num = self.address.split(":")[2].split(".")[0].upper()
        self.fun = self.address.split(":")[2].split(".")[1].upper()

    def __str__(self) -> str:
        return self.address

    def __eq__(self, other):
        if type(other) == PCIAddress:
            return self.address.upper() == other.address.upper()
        elif type(other) == str:
            return self.address.upper() == other.upper()
        else:
            return False

    def __ne__(self, other):
        if type(other) == PCIAddress:
            return self.address.upper() != other.address.upper()
        elif type(other) == str:
            return self.address.upper() != other.upper()
        else:
            return True


class PCIDevice(object):
    """
    Mapper Class for PCI device.
    """

    def __init__(self, data: typing.List[str]) -> None:
        self.data = data

        self.pci_address = PCIAddress(address=self.data[0].split(" ")[0].strip())
        self.pci_link: typing.Optional[PCILink] = None
        self.pci_bus: typing.Optional[PCIBus] = None

        self.type = (
            self.data[0].replace(f"{self.pci_address}", "").split(":")[0].strip()
        )
        self.name = (
            self.data[0].replace(f"{self.pci_address}", "").split(":")[1].strip()
        )

        self.control: typing.Optional[str] = None
        self.status: typing.Optional[str] = None
        self.capabilities: typing.List[str] = []

        self.latency: typing.Optional[str] = None
        self.numa_node: typing.Optional[str] = None
        self.bus: typing.Optional[str] = None
        self.io_behind_bridge: typing.Optional[str] = None
        self.memory_behind_bridge: typing.Optional[str] = None
        self.prefetchable_memory_behind_bridge: typing.Optional[str] = None
        self.secondary_status: typing.Optional[str] = None
        self.bridge_ctl: typing.Optional[str] = None
        self.flags: typing.Optional[str] = None
        self.devcap: typing.Optional[str] = None
        self.devcap2: typing.Optional[str] = None
        self.devctl: typing.Optional[str] = None
        self.devctl2: typing.Optional[str] = None
        self.devsta: typing.Optional[str] = None
        self.lnkcap: typing.Optional[str] = None
        self.lnkctl: typing.Optional[str] = None
        self.lnkctl2: typing.Optional[str] = None
        self.lnksta: typing.Optional[str] = None
        self.lnksta2: typing.Optional[str] = None
        self.rootctl: typing.Optional[str] = None
        self.rootcap: typing.Optional[str] = None
        self.rootsta: typing.Optional[str] = None
        self.atomic_ops_cap: typing.Optional[str] = None
        self.atomic_ops_ctl: typing.Optional[str] = None
        self.transmit_margin: typing.Optional[str] = None
        self.compliance_deemphasis: typing.Optional[str] = None
        self.uesta: typing.Optional[str] = None
        self.uemsk: typing.Optional[str] = None
        self.uesvrt: typing.Optional[str] = None
        self.cesta: typing.Optional[str] = None
        self.cemsk: typing.Optional[str] = None
        self.aercap: typing.Optional[str] = None
        self.header_log: typing.Optional[str] = None
        self.root_cmd: typing.Optional[str] = None
        self.error_src: typing.Optional[str] = None
        self.interrupt: typing.Optional[str] = None
        self.address: typing.Optional[str] = None
        self.kernel_driver_in_use: typing.Optional[str] = None

        for entry in self.data[3 : len(self.data) + 1]:
            name = (
                entry.split(":")[0]
                .replace(" ", "")
                .replace("/", "")
                .replace("[", "")
                .replace("]", "")
                .replace("-", "")
                .lower()
            )
            content = entry.replace(entry.split(":")[0] + ":", "").strip()

            if name != "capabilities":
                is_assigned = False
                for field in self.__dict__.keys():
                    if field.replace("_", "") == name and getattr(self, field) is None:
                        setattr(self, field, content)
                        is_assigned = True

                if is_assigned is False:
                    setattr(self, f"aux_{name}", content)

            elif name == "capabilities":
                self.capabilities.append(content)

            elif name == "bus":
                self.bus = PCIBus(data=content)

        if self.lnkcap is not None and self.lnksta is not None:
            self.pci_link = PCILink(self.lnkcap, self.lnksta)

        if self.bus is not None:
            self.pci_bus = PCIBus(self.bus)

    def match(self, **kwargs) -> bool:
        result = True
        for key, value in kwargs.items():
            if key in self.__dir__() and result is not False:
                if (
                    type(value) is str
                    and value.startswith("*")
                    and type(getattr(self, key)) is str
                ):
                    result = (
                        value.replace("*", "").upper() in getattr(self, key).upper()
                    )
                else:
                    result = value == getattr(self, key)
            else:
                result = False
        return result

    def _check_capabilities(self, value: str) -> bool:
        result = False
        for line in self.capabilities:
            if value in line:
                result = True
        return result

    @property
    def device_serial_number(self) -> typing.Optional[str]:
        result = None
        for line in self.capabilities:
            if "Device Serial Number" in line:
                result = line.split("Device Serial Number")[-1].strip()
        return result

    @property
    def subsystem(self) -> typing.Optional[str]:
        result = None
        for line in self.capabilities:
            if "Subsystem:" in line:
                result = line.split("Subsystem:")[-1].strip()
        return result

    @property
    def is_host_bridge(self) -> bool:
        return self.type == "Host bridge"

    @property
    def is_root_port(self) -> bool:
        return self._check_capabilities(value="Root Port")

    @property
    def is_downstream(self) -> bool:
        return (
            self._check_capabilities(value="Downstream Port")
            and self.type == "PCI bridge"
        )

    @property
    def is_upstream(self) -> bool:
        return (
            self._check_capabilities(value="Upstream Port")
            and self.type == "PCI bridge"
        )

    @property
    def is_endpoint(self) -> bool:
        return (
            not self.is_host_bridge
            and not self.is_root_port
            and not self.is_upstream
            and not self.is_downstream
        )

    @property
    def addr_dom(self) -> str:
        return self.pci_address.dom

    @property
    def addr_bus(self) -> str:
        return self.pci_address.bus

    @property
    def addr_num(self) -> str:
        return self.pci_address.num

    @property
    def addr_fun(self) -> str:
        return self.pci_address.fun

    def __str__(self) -> str:
        link = str(self.pci_link) if self.pci_link is not None else ""
        return f"{self.pci_address} {self.type} {self.name} {link}"

    def __eq__(self, other) -> bool:
        if type(other) == PCIDevice:
            return (
                self.address == other.address
                and self.type == other.type
                and self.name == other.name
            )
        else:
            return False

    def __ne__(self, other) -> bool:
        if type(other) == PCIDevice:
            return (
                self.address != other.address
                or self.type != other.type
                or self.name != other.name
            )
        else:
            return True


# noinspection PyBroadException
class PCIParser(object):
    def __init__(self, data: typing.List[typing.List[str]]) -> None:
        self.data = data
        self.devices: typing.List[PCIDevice] = []
        self.process_data(data=self.data)

    def process_data(self, data: typing.List[typing.List[str]]) -> None:
        for entry in data:
            result = self.parce_single_device(data=entry)
            if result is not None:
                self.devices.append(PCIDevice(data=result))

    @staticmethod
    def parce_single_device(
        data: typing.List[str],
    ) -> typing.Optional[typing.List[str]]:
        result = None
        if data is not None:
            temp_list = []
            line_counter = 0
            for line in data:
                line = line.replace("\t", "").replace("\r", "").replace("\n", "")
                if len(line) > 4 and "pcilib:" not in line:
                    temp_list.append(line)
                    if ":" not in line and "lspci" not in line:
                        temp_list[(line_counter - 1) : (line_counter + 1)] = [
                            " ".join(temp_list[(line_counter - 1) : (line_counter + 1)])
                        ]
                    else:
                        line_counter += 1

            if len(temp_list) != 0:
                result = temp_list
        return result
