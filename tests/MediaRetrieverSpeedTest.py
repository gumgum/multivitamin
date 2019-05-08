from multivitamin.media import MediaRetriever

from tabulate import tabulate
from datetime import datetime
import random
from tqdm import tqdm

print("SPEED TEST!!!")

VIDEO_URL = "https://s3.amazonaws.com/video-ann-testing/NHL_GAME_VIDEO_NJDMTL_M2_NATIONAL_20180401_1520698069177.t.mp4"
VIDEO_CODEC_PROB_1 = "https://s3.amazonaws.com/video-ann/538_Pelicans+vs+Thunder+11%3A5-fhj713lbrhi.30-31.mp4"
VIDEO_CODEC_PROB_2 = "https://s3.amazonaws.com/gumgum-sports-analyst-data/media-files/Replay%20Video%20Capture_2018-11-16_11.52.51-2816an1tb0v.mp4"
VIDEO_CODEC_PROB_3 = "https://s3.amazonaws.com/gumgum-sports-analyst-data/media-files/1%3A3%20Houston%20Rockets%20at%20Golden%20State%20Warriors-6tgm4my1dr6.mp4"

VIDEO_URLS = [VIDEO_URL, VIDEO_CODEC_PROB_1, VIDEO_CODEC_PROB_2, VIDEO_CODEC_PROB_3]


def create_media_retrievers(url):
    efficient_mr = MediaRetriever(VIDEO_URL)
    fast_mr = MediaRetriever(VIDEO_URL, limitation="cpu")
    return efficient_mr, fast_mr


def _benchmark_get_frame(mrs, num_samples=100, num_tests=100):
    averaged_results = []
    random_samples = np.random.rand(num_samples)
    for mr in mrs:
        random_tstamps = random_samples * mr.length
        results = []
        for _ in tqdm(range(num_tests), desc="get frame"):
            start = datetime.now()
            for tstamp in random_samples:
                mr.get_frame(tstamp)
            end = datetime.now()
            x = end - start
            results.append(x.total_seconds())
        averaged_results.append(np.average(results))
    return averaged_results


def _benchmark_frames_iterator(mrs, sample_rate, num_samples=100, num_tests=100):
    averaged_results = []
    random_sample = random.random()
    for mr in mrs:
        random_start = random_sample * mr.length
        results = []
        for _ in tqdm(range(num_tests), desc="iterator test"):
            start = datetime.now()
            for idx, _ in enumerate(
                mr.get_frames_iterator(
                    sample_rate=sample_rate, start_tstamp=random_start
                )
            ):
                if idx >= num_samples:
                    break
            end = datetime.now()
            x = end - start
            results.append(x.total_seconds())
        averaged_results.append(np.average(results))
    return averaged_results


rs = []
for url in VIDEO_URLS:
    mrs = create_media_retrievers(url)
    headers = ["Test Type", "Efficient", "Fast"]

    test_order = [
        "get_frame",
        "iterator (sample_rate=100)",
        "iterator (sample_rate=2)",
        "iterator (sample_rate=0.2)",
    ]
    get_frame_results = _benchmark_get_frame(mrs, num_tests=1)
    iterator_results1 = _benchmark_frames_iterator(mrs, 100, num_tests=5)
    iterator_results2 = _benchmark_frames_iterator(mrs, 2, num_tests=5)
    iterator_results3 = _benchmark_frames_iterator(mrs, 0.2, num_tests=5)

    results = list(
        zip(
            test_order,
            get_frame_results,
            iterator_results1,
            iterator_results2,
            iterator_results3,
        )
    )
    rs.append(results)

print("\n" * 4)
for url, results in zip(VIDEO_URLS, rs):
    print("\nRESULTS ON: " + url)
    print(tabulate(results, headers=headers))
