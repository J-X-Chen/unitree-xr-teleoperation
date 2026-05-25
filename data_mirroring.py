#!/usr/bin/env python3
import argparse
import copy
import json
import re
import shutil
import subprocess
from pathlib import Path


EPISODE_RE = re.compile(r"^episode_(\d+)$")
IMAGE_SUFFIXES = {
    ".bmp",
    ".jpeg",
    ".jpg",
    ".png",
    ".tif",
    ".tiff",
    ".webp",
}
IMAGE_DIR_NAMES = {"colors", "depths", "depth"}

# G1 29-dof arm order:
# shoulder_pitch, shoulder_roll, shoulder_yaw, elbow,
# wrist_roll, wrist_pitch, wrist_yaw.
ARM_7_SIGN = [1, -1, -1, 1, -1, 1, -1]
ARM_5_SIGN = [1, -1, -1, 1, -1]

# G1 body motor order in robot_arm.G1_29_JointIndex.
LEG_6_SIGN = [1, -1, -1, 1, 1, -1]
WAIST_3_SIGN = [-1, -1, 1]

# Dex3 data order is asymmetric in the controller:
# left:  thumb0, thumb1, thumb2, middle0, middle1, index0, index1
# right: thumb0, thumb1, thumb2, index0, index1, middle0, middle1
DEX3_SWAP_PERM = [0, 1, 2, 5, 6, 3, 4]
DEX3_SIGN = [1, -1, -1, -1, -1, -1, -1]

# actions.body.qpos is recorded as [forward/back, left/right, yaw].
BODY_ACTION_3_SIGN = [1, -1, -1]
IMAGE_BACKEND = None


def _episode_number(path):
    match = EPISODE_RE.match(path.name)
    if match is None:
        return None
    return int(match.group(1))


def _list_episodes(root):
    episodes = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        number = _episode_number(child)
        if number is not None:
            episodes.append((number, child))
    return sorted(episodes, key=lambda item: item[0])


def _is_image(path):
    return path.suffix.lower() in IMAGE_SUFFIXES


def _get_image_backend():
    global IMAGE_BACKEND
    if IMAGE_BACKEND is not None:
        return IMAGE_BACKEND

    try:
        import cv2  # type: ignore
    except ImportError:
        pass
    else:
        IMAGE_BACKEND = ("cv2", cv2)
        return IMAGE_BACKEND

    try:
        from PIL import Image, ImageOps  # type: ignore
    except ImportError:
        pass
    else:
        IMAGE_BACKEND = ("pil", (Image, ImageOps))
        return IMAGE_BACKEND

    convert_bin = shutil.which("convert")
    if convert_bin is not None:
        IMAGE_BACKEND = ("imagemagick", convert_bin)
        return IMAGE_BACKEND

    raise RuntimeError(
        "Mirroring image files requires opencv-python, Pillow, or ImageMagick "
        "(`convert`) to be available."
    )


def _apply_sign(values, signs):
    if len(values) != len(signs):
        return list(values)
    mirrored = []
    for value, sign in zip(values, signs):
        mirrored.append(-value if sign < 0 else value)
    return mirrored


def _permute(values, perm):
    if len(values) != len(perm):
        return list(values)
    return [values[index] for index in perm]


def _mirror_arm_values(values):
    if len(values) == 7:
        return _apply_sign(values, ARM_7_SIGN)
    if len(values) == 5:
        return _apply_sign(values, ARM_5_SIGN)
    return list(values)


def _mirror_ee_values(values):
    if len(values) == 7:
        return _apply_sign(_permute(values, DEX3_SWAP_PERM), DEX3_SIGN)
    return list(values)


def _mirror_body_values(values):
    if len(values) == 35:
        left_leg = values[0:6]
        right_leg = values[6:12]
        waist = values[12:15]
        left_arm = values[15:22]
        right_arm = values[22:29]
        unused = values[29:35]
        return (
            _apply_sign(right_leg, LEG_6_SIGN)
            + _apply_sign(left_leg, LEG_6_SIGN)
            + _apply_sign(waist, WAIST_3_SIGN)
            + _apply_sign(right_arm, ARM_7_SIGN)
            + _apply_sign(left_arm, ARM_7_SIGN)
            + list(unused)
        )
    if len(values) == 3:
        return _apply_sign(values, BODY_ACTION_3_SIGN)
    return list(values)


def _mirror_component(component, mirror_values):
    mirrored = copy.deepcopy(component)
    if not isinstance(mirrored, dict):
        return mirrored
    for key in ("qpos", "qvel", "torque"):
        values = mirrored.get(key)
        if isinstance(values, list):
            mirrored[key] = mirror_values(values)
    return mirrored


def _mirror_pair(container, left_key, right_key, mirror_values):
    left = container.get(left_key)
    right = container.get(right_key)
    if left is None or right is None:
        return
    container[left_key] = _mirror_component(right, mirror_values)
    container[right_key] = _mirror_component(left, mirror_values)


def _swap_left_right_text(value):
    if isinstance(value, str):
        def replace(match):
            word = match.group(0)
            replacement = "right" if word.lower() == "left" else "left"
            if word.isupper():
                return replacement.upper()
            if word[:1].isupper():
                return replacement.capitalize()
            return replacement

        return re.sub(r"\b(left|right)\b", replace, value, flags=re.IGNORECASE)
    if isinstance(value, list):
        return [_swap_left_right_text(item) for item in value]
    if isinstance(value, dict):
        return {key: _swap_left_right_text(item) for key, item in value.items()}
    return value


def _mirror_data_json(data):
    mirrored = copy.deepcopy(data)
    if "text" in mirrored:
        mirrored["text"] = _swap_left_right_text(mirrored["text"])

    for item in mirrored.get("data", []):
        for section_name in ("states", "actions"):
            section = item.get(section_name)
            if not isinstance(section, dict):
                continue

            _mirror_pair(section, "left_arm", "right_arm", _mirror_arm_values)
            _mirror_pair(section, "left_ee", "right_ee", _mirror_ee_values)

            body = section.get("body")
            if isinstance(body, dict):
                section["body"] = _mirror_component(body, _mirror_body_values)

    return mirrored


def _mirror_image_file(source_path, target_path):
    backend_name, backend = _get_image_backend()
    if backend_name == "cv2":
        cv2 = backend
        image = cv2.imread(str(source_path), cv2.IMREAD_UNCHANGED)
        if image is None:
            raise RuntimeError(f"Failed to read image: {source_path}")
        mirrored = cv2.flip(image, 1)
        if not cv2.imwrite(str(target_path), mirrored):
            raise RuntimeError(f"Failed to write mirrored image: {target_path}")
        return

    if backend_name == "pil":
        Image, ImageOps = backend
        with Image.open(source_path) as image:
            ImageOps.mirror(image).save(target_path)
        return

    result = subprocess.run(
        [backend, str(source_path), "-flop", str(target_path)],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"ImageMagick failed to mirror {source_path}: {result.stderr.strip()}"
        )


def _mirror_image_tree(source_dir, target_dir):
    image_count = 0
    copied_count = 0
    target_dir.mkdir(parents=True, exist_ok=False)

    for source_path in sorted(source_dir.rglob("*")):
        relative = source_path.relative_to(source_dir)
        target_path = target_dir / relative
        if source_path.is_dir():
            target_path.mkdir(parents=True, exist_ok=True)
            continue

        target_path.parent.mkdir(parents=True, exist_ok=True)
        if not _is_image(source_path):
            shutil.copy2(source_path, target_path)
            copied_count += 1
            continue

        _mirror_image_file(source_path, target_path)
        image_count += 1

    return image_count, copied_count


def _copy_or_mirror_episode_files(source_episode, target_episode):
    image_count = 0
    copied_count = 0
    target_episode.mkdir(parents=True, exist_ok=False)

    for source_path in sorted(source_episode.iterdir()):
        target_path = target_episode / source_path.name
        if source_path.name == "data.json":
            continue
        if source_path.is_dir():
            if source_path.name in IMAGE_DIR_NAMES:
                mirrored_images, copied_files = _mirror_image_tree(source_path, target_path)
                image_count += mirrored_images
                copied_count += copied_files
            else:
                shutil.copytree(source_path, target_path)
                copied_count += sum(1 for child in target_path.rglob("*") if child.is_file())
        else:
            shutil.copy2(source_path, target_path)
            copied_count += 1

    return image_count, copied_count


def _mirror_episode(source_episode, target_episode):
    if target_episode.exists():
        raise FileExistsError(f"Target episode already exists: {target_episode}")

    data_path = source_episode / "data.json"
    if not data_path.exists():
        raise FileNotFoundError(f"Missing data.json: {data_path}")

    image_count, copied_count = _copy_or_mirror_episode_files(source_episode, target_episode)

    with data_path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    mirrored_data = _mirror_data_json(data)
    with (target_episode / "data.json").open("w", encoding="utf-8") as file:
        json.dump(mirrored_data, file, ensure_ascii=False, indent=4)
        file.write("\n")

    return {
        "frames": len(mirrored_data.get("data", [])),
        "mirrored_images": image_count,
        "copied_files": copied_count,
    }


def _warn_episode_gaps(episodes):
    if not episodes:
        return []
    numbers = [number for number, _ in episodes]
    expected = set(range(numbers[0], numbers[-1] + 1))
    missing = sorted(expected - set(numbers))
    warnings = []
    if numbers[0] != 0:
        warnings.append(f"first episode is episode_{numbers[0]:04d}, not episode_0000")
    if missing:
        joined = ", ".join(f"episode_{number:04d}" for number in missing)
        warnings.append(f"missing episode(s) in sequence: {joined}")
    return warnings


def _build_target_plan(source_episodes, output_root):
    existing_targets = _list_episodes(output_root) if output_root.exists() else []
    next_number = existing_targets[-1][0] + 1 if existing_targets else 0
    return [
        (source_episode, output_root / f"episode_{next_number + offset:04d}")
        for offset, (_, source_episode) in enumerate(source_episodes)
    ]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Mirror teleoperation episodes and append them as new episodes."
    )
    parser.add_argument(
        "--root",
        required=True,
        type=Path,
        help="Input episode root, for example teleop/place_water/place_water.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Where mirrored episodes are written. Defaults to --root.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the source -> target episode plan without writing files.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    root = args.root.expanduser().resolve()
    output_root = (args.output_root or args.root).expanduser().resolve()

    if not root.exists():
        raise FileNotFoundError(f"Root does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Root is not a directory: {root}")

    source_episodes = _list_episodes(root)
    if not source_episodes:
        raise RuntimeError(f"No episode_XXXX directories found under: {root}")

    output_root.mkdir(parents=True, exist_ok=True)
    plan = _build_target_plan(source_episodes, output_root)
    collisions = [target for _, target in plan if target.exists()]
    if collisions:
        collision_list = ", ".join(str(path) for path in collisions)
        raise FileExistsError(f"Target episode(s) already exist: {collision_list}")

    for warning in _warn_episode_gaps(source_episodes):
        print(f"Warning: {warning}")

    print(f"Found {len(source_episodes)} episode(s) under {root}")
    for source_episode, target_episode in plan:
        print(f"{source_episode.name} -> {target_episode.name}")

    if args.dry_run:
        print("Dry run only; no files written.")
        return

    for source_episode, target_episode in plan:
        stats = _mirror_episode(source_episode, target_episode)
        print(
            f"Saved {target_episode}: "
            f"{stats['frames']} frame(s), "
            f"{stats['mirrored_images']} mirrored image(s), "
            f"{stats['copied_files']} copied file(s)"
        )


if __name__ == "__main__":
    main()
