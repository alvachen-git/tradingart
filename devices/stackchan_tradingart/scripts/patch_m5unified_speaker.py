from pathlib import Path

Import("env")

PROJECT_DIR = Path(env.subst("$PROJECT_DIR"))
SPEAKER_CPP = PROJECT_DIR / ".pio" / "libdeps" / "stackchan" / "M5Unified" / "src" / "utility" / "Speaker_Class.cpp"

STACK_OLD = "size_t stack_size = 1280 + (_cfg.dma_buf_len * sizeof(uint32_t));"
STACK_NEW = "size_t stack_size = 6144 + (_cfg.dma_buf_len * sizeof(uint32_t));"
BUFFER_OLD = "    int32_t* sound_buf32 = (int32_t*)alloca(dma_buf_len * sizeof(int32_t));"
BUFFER_NEW = """    int32_t* sound_buf32 = (int32_t*)malloc(dma_buf_len * sizeof(int32_t));
    if (sound_buf32 == nullptr)
    {
      self->_task_running = false;
      self->_task_handle = nullptr;
      vTaskDelete(nullptr);
      return;
    }"""
BUFFER_FREE_OLD = """    self->_task_handle = nullptr;
    vTaskDelete(nullptr);"""
BUFFER_FREE_NEW = """    free(sound_buf32);
    self->_task_handle = nullptr;
    vTaskDelete(nullptr);"""


def patch_speaker_source():
    if not SPEAKER_CPP.exists():
        print("[stackchan] M5Unified speaker source not present yet; skipping speaker patch")
        return

    text = SPEAKER_CPP.read_text(encoding="utf-8")
    changed = False

    if STACK_NEW not in text:
        if STACK_OLD not in text:
            raise RuntimeError(
                "M5Unified Speaker_Class.cpp stack-size pattern not found; "
                "inspect the library before building StackChan voice firmware."
            )
        text = text.replace(STACK_OLD, STACK_NEW, 1)
        changed = True

    if BUFFER_NEW not in text:
        if BUFFER_OLD not in text:
            raise RuntimeError(
                "M5Unified Speaker_Class.cpp alloca buffer pattern not found; "
                "inspect the library before building StackChan voice firmware."
            )
        text = text.replace(BUFFER_OLD, BUFFER_NEW, 1)
        changed = True

    if BUFFER_FREE_NEW not in text:
        if BUFFER_FREE_OLD not in text:
            raise RuntimeError(
                "M5Unified Speaker_Class.cpp task cleanup pattern not found; "
                "inspect the library before building StackChan voice firmware."
            )
        text = text.replace(BUFFER_FREE_OLD, BUFFER_FREE_NEW, 1)
        changed = True

    if changed:
        SPEAKER_CPP.write_text(text, encoding="utf-8")
        print("[stackchan] Patched M5Unified speaker stack and heap playback buffer")
    else:
        print("[stackchan] M5Unified speaker patch already applied")


patch_speaker_source()


def before_build(source, target, env):
    patch_speaker_source()


env.AddPreAction("buildprog", before_build)
