"""文件检索模型参数；身份、权限与存储边界只存在于宿主后端回调。"""
from pydantic import BaseModel, ConfigDict, Field, field_validator
from ...json_codec import ModelActionRejected


class RetrievalModel(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, str_strip_whitespace=True)


class SearchFilesArguments(RetrievalModel):
    """根据文件名称、内容线索或精确标签查找文件。保留必要条件、否定和替代关系，不补造未知信息。可以在已有结果集内继续查找。返回结果集引用、数量及查询说明；需要了解具体文件时使用 view_file_candidates。检索命中不代表原文已经核实。"""
    query: str = Field(
        strict=True,
        min_length=1,
        max_length=10000,
        description='完整自然语言查找需求，保留名称、内容、精确标签、必要条件、否定及 AND/OR 关系；按精确标签或业务编号查找时提供原始完整值，不猜测或改写。',
    )
    result_id: str | None = Field(
        default=None,
        strict=True,
        min_length=1,
        description='可选的 search_files 返回的完整最终结果集 ID；传入后仅在其中继续查询。省略或 null 使用当前可访问范围；失效时报错，不回退全库。',
    )


class NameQuery(RetrievalModel):
    """按文件名称关键词查找；保留已知主体、标的、编号，不补造名称。可在已有集合内继续收窄，命中不证明内容条件成立。"""
    query: str = Field(
        strict=True,
        min_length=1,
        max_length=1000,
        description='简短的文件名称或名称关键词，例如“设备采购协议”；保留已知名称线索，不写成问题或摘要。',
    )
    result_id: str | None = Field(
        default=None,
        strict=True,
        min_length=1,
        description='本次检索返回的完整候选结果集 ID；传入限定集合，省略或 null 回到本次初始范围。不能传文件 ID，失效不回退。',
    )


class SummaryQuery(RetrievalModel):
    """按摘要内容查找，关键词与语义联合召回。根据已知线索拟写简短正式摘要，保留否定和条件，不补造主体、金额、日期或约定。摘要命中仍须原文核实。"""
    query: str = Field(
        strict=True,
        min_length=1,
        max_length=10000,
        description='根据已知内容拟写的简短正式摘要，例如“设备采购，约定验收后付款及质量保证”；只整理用户线索，不补充未知事实。',
    )
    result_id: str | None = Field(
        default=None,
        strict=True,
        min_length=1,
        description='本次检索返回的完整候选结果集 ID；传入后在该集合摘要内查询，省略或 null 回到初始范围。失效引用报错。',
    )


class TagQuery(RetrievalModel):
    """按单个完整标签或精确业务标识查找关联文件。一份文件可包含多个标签，同一标签也可对应多份文件。保留原值、大小写和空格，不分词、不拆分或猜测；可在已有结果集内筛选，并始终遵守本次可访问范围。"""
    model_config = ConfigDict(extra='forbid', frozen=True, str_strip_whitespace=False)

    tag: str = Field(
        strict=True,
        min_length=1,
        max_length=512,
        description='一个非空白的完整标签或精确标识字符串，最多 512 字符；精确匹配文件标签列表中的一个元素。保留首尾空格和大小写，不传数组，不拆分逗号或看起来像 JSON 的字符串。',
    )
    result_id: str | None = Field(
        default=None,
        strict=True,
        min_length=1,
        description='本次检索返回的完整候选结果集 ID；传入后只在该集合内匹配标签，省略或 null 使用本次初始范围。失效引用报错，不扩大范围。',
    )

    @field_validator('tag')
    @classmethod
    def nonblank_tag(cls, value: str) -> str:
        if not value.strip():
            raise ModelActionRejected(
                '标签全部为空白，未执行查询。',
                '提供已知的完整标签原值；不要猜测或拆分标签。',
            )
        return value


class UnionResults(RetrievalModel):
    """满足两组条件任意一组时，分别在相同初始范围查询，再合并两结果集的并集并去重。不要先串行收窄来表达 OR，不为抬分反复合并同源结果。"""
    left_result_id: str = Field(
        strict=True,
        min_length=1,
        description='本次检索第一张非空候选集合的完整 ID；不能传文件 ID 或外部结果引用。',
    )
    right_result_id: str = Field(
        strict=True,
        min_length=1,
        description='本次检索第二张非空候选集合的完整 ID；与左表相同时复用原表。一路为空直接选择另一路，无需合并。',
    )


class FinishRetrieval(RetrievalModel):
    """选择本次最终候选集合并结束检索。不提供结果集表示未找到合适候选；说明未核实条件、未覆盖分支及召回限制，不虚构原文事实。"""
    result_id: str | None = Field(
        default=None,
        strict=True,
        min_length=1,
        description='最终提交的完整候选结果集 ID；省略或 null 表示没有合适候选，伪造或失效引用不是空结果。',
    )
    explanation: str = Field(
        strict=True,
        min_length=1,
        max_length=2000,
        description='简洁说明使用的条件与尚未核实的限制；失败不能解释为没有相关文件。',
    )


class ViewFileCandidatesArguments(RetrievalModel):
    """分页查看文件检索的最终候选，每条包含文件 ID、文件名称以及后端提供的可选摘要、标签与扩展元信息。省略页码时首次为第一页，随后下一页；指定页码可重看。候选不表示原文已核实。"""
    result_id: str = Field(
        strict=True,
        min_length=1,
        description='search_files 返回的完整最终结果集 ID，不接受文件 ID、其他任务引用或自行编造的标识。',
    )
    page: int | None = Field(
        default=None,
        strict=True,
        ge=1,
        description='从 1 开始的页码；省略或 null 首次查看第一页，之后下一页。末页不循环，显式页码可以重看。',
    )
