# for motion switcher
from unitree_sdk2py.core.channel import ChannelFactoryInitialize
from unitree_sdk2py.comm.motion_switcher.motion_switcher_client import MotionSwitcherClient
# for loco client
from unitree_sdk2py.g1.loco.g1_loco_client import LocoClient
import time
import traceback

import logging_mp
logger_mp = logging_mp.getLogger(__name__)

# MotionSwitcher used to switch mode between debug mode and ai mode
class MotionSwitcher:
    def __init__(self):
        self.msc = MotionSwitcherClient()
        self.msc.SetTimeout(1.0)
        self.msc.Init()

    def Enter_Debug_Mode(self):
        phase = "init"
        try:
            phase = "CheckMode"
            status, result = self.msc.CheckMode()
            logger_mp.info(f"[MotionSwitcher] CheckMode response: status={status}, result={result}")
            if not isinstance(result, dict):
                return status, {
                    "error": "CheckMode returned unexpected result type",
                    "phase": phase,
                    "result_type": type(result).__name__,
                    "result": result,
                }

            while result.get('name'):
                phase = f"ReleaseMode({result.get('name')})"
                release_result = self.msc.ReleaseMode()
                logger_mp.info(f"[MotionSwitcher] ReleaseMode response: {release_result}")
                phase = "CheckMode after ReleaseMode"
                status, result = self.msc.CheckMode()
                logger_mp.info(f"[MotionSwitcher] CheckMode response: status={status}, result={result}")
                if not isinstance(result, dict):
                    return status, {
                        "error": "CheckMode returned unexpected result type after ReleaseMode",
                        "phase": phase,
                        "result_type": type(result).__name__,
                        "result": result,
                    }
                time.sleep(1)
            return status, result
        except Exception as e:
            detail = {
                "error": "Enter_Debug_Mode exception",
                "phase": phase,
                "exception_type": type(e).__name__,
                "exception": str(e),
                "traceback": traceback.format_exc(),
            }
            logger_mp.error(f"[MotionSwitcher] Enter_Debug_Mode failed: {detail}")
            return None, detail
    
    def Exit_Debug_Mode(self):
        try:
            status, result = self.msc.SelectMode(nameOrAlias='ai')
            return status, result
        except Exception as e:
            detail = {
                "error": "Exit_Debug_Mode exception",
                "exception_type": type(e).__name__,
                "exception": str(e),
                "traceback": traceback.format_exc(),
            }
            logger_mp.error(f"[MotionSwitcher] Exit_Debug_Mode failed: {detail}")
            return None, detail

class LocoClientWrapper:
    def __init__(self):
        self.client = LocoClient()
        self.client.SetTimeout(0.0001)
        self.client.Init()

    def Enter_Damp_Mode(self):
        self.client.Damp()

    def Damp(self):
        self.Enter_Damp_Mode()
    
    def Move(self, vx, vy, vyaw):
        self.client.Move(vx, vy, vyaw, continous_move=False)

    def close(self):
        try:
            self.client.Move(0.0, 0.0, 0.0, continous_move=False)
        except Exception as e:
            logger_mp.warning(f"[LocoClientWrapper] stop move failed during close: {e}")

if __name__ == '__main__':
    ChannelFactoryInitialize(1) # 0 for real robot, 1 for simulation
    ms = MotionSwitcher()
    status, result = ms.Enter_Debug_Mode()
    print("Enter debug mode:", status, result)
    time.sleep(5)
    status, result = ms.Exit_Debug_Mode()
    print("Exit debug mode:", status, result)
    time.sleep(2)
