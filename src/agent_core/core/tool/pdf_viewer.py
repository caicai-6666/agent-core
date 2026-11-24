"""通用 PDF 逐页查看工具；集成 CachePool、可折叠页面展示与末尾反馈渲染钩子。"""
from __future__ import annotations

from functools import partial
from textwrap import dedent

from pydantic import BaseModel, ConfigDict, Field

from ..file.pdf_viewer import PDFViewer
from ..model import AgentDependencyError
from .feedback import with_feedback
from .hooks import FeedbackOnlyHook
from .progress import ToolProgress
from .registry import RegisteredTool
from .result import ToolPageUnavailable


def render_pdf_feedback(arguments, execution, context, *, title: str = 'PDF 文件查看'):
    """末尾纯展示钩子：将 PDF 查看的结构化结果渲染为模型可读正文。"""
    result = execution.result.tool_result
    if execution.result.status != 'succeeded':
        text = dedent("""
            ### {title}

            状态：查看未完成，阅读位置未推进。
            原因：{reason}
            下一步：{required_action}
        """).strip().format(
            title=title,
            reason=result['reason'],
            required_action=result['required_action'],
        )
        return with_feedback(execution, text)

    visible_action = (
        '可省略 page_number 继续查看下一页，或指定页码重看；末页不循环。'
        if result['has_next']
        else '已到末页；如需重看，请明确指定有效页码。'
    )
    name_line = f"\n文件名称：{result['file_name']}" if result.get('file_name') else ''
    template = dedent("""
        ### {title}

        状态：查看记录已提交
        文件 ID：{file_id}{name_line}
        当前页：第 {page_number} / {page_count} 页
        资料状态：{state}
        下一步：{action}
    """).strip()
    values = {
        'title': title,
        'file_id': result['file_id'],
        'name_line': name_line,
        'page_number': result['page_number'],
        'page_count': result['page_count'],
    }
    return with_feedback(
        execution,
        template.format(**values, state='本页图像见随后提供的资料。', action=visible_action),
        hidden=template.format(
            **values,
            state='本页图像当前未展示，不能从查看记录推断原文。',
            action='需要核对时使用此文件 ID 和页码重新查看。',
        ),
        unavailable=template.format(
            **values,
            state='本页图像当前无法读取，不能推断原文。',
            action='请重新核对文件标识并调用查看工具。',
        ),
    )


class ViewPDFArguments(BaseModel):
    """当你需要阅读 PDF 文件的原始页面、核对具体内容或继续翻页时，使用这个工具。一次返回一页图像及当前页、总页数，帮助依据实际页面作答。将上下文或前置工具返回的完整文件标识原样填入 file_id。可以指定页码，也可以省略以继续查看下一页；查看失败时会说明原因，阅读位置保持不变。需要后续继续使用的事实、来源和限制应保存到工作区；需要核对细节时重新查看，不能仅凭查看记录推断原文。"""

    model_config = ConfigDict(extra='forbid', strict=True, frozen=True)
    file_id: str = Field(
        min_length=1,
        pattern=r'\S',
        description='目标 PDF 文件的完整非空标识，原样复制上下文或前置工具返回的文件标识；不要填写空字符串，也不要自行猜测不存在的标识。',
    )
    page_number: int | None = Field(
        default=None,
        gt=0,
        description='要查看的页码，填从 1 开始的整数，按 PDF 页面顺序计数，不按正文印刷页码。省略或填 null 时，查看上次成功查看该文件的下一页；从未查看则打开第 1 页。例如上次看了第 3 页，不填就打开第 4 页；重看第 3 页需填写 3。超过总页数会报错，阅读位置保持不变。',
    )


def build_pdf_view_registration(
    viewer: PDFViewer,
    *,
    name: str = 'view_pdf',
    description: str | None = None,
    title: str = 'PDF 文件查看',
) -> RegisteredTool:
    """将 PDFViewer 绑定为可注册的 view_pdf 工具，自动声明对应的 CachePool 读写权限。"""
    if not isinstance(viewer, PDFViewer):
        raise TypeError('viewer 必须为 PDFViewer 实例')

    def prepare(tool_name, raw, workspace, arguments):
        return workspace

    async def execute(arguments: ViewPDFArguments):
        return await viewer.view(**arguments.model_dump())

    async def resolve(reference):
        try:
            return await viewer.resolve_page(reference)
        except AgentDependencyError:
            raise
        except ToolPageUnavailable:
            raise
        except Exception:
            raise ToolPageUnavailable('PDF 图像资料当前无法读取。') from None

    progress_factory = None
    if viewer.has_file_name:
        async def progress(arguments: ViewPDFArguments) -> ToolProgress:
            prefix = '正在查看 PDF 文件'
            try:
                file_name = await viewer.get_file_name(arguments.file_id)
            except AgentDependencyError:
                raise
            except Exception:
                file_name = None
            if file_name:
                text = ''.join(char for char in ' '.join(file_name.split()) if char.isprintable())
                if text:
                    limit = 2000 - len(prefix) - 1
                    if len(text) > limit:
                        text = text[: limit - 1] + '…'
                    prefix += '：' + text
            return ToolProgress(type='thinking', message=prefix)

        progress_factory = progress

    tool_description = description.strip() if isinstance(description, str) and description.strip() else (ViewPDFArguments.__doc__ or '').strip()
    return RegisteredTool(
        name,
        tool_description,
        ViewPDFArguments,
        prepare,
        executor=execute,
        return_types=('foldable',),
        page_resolver=resolve,
        progress=ToolProgress(type='thinking', message='正在查看 PDF 文件'),
        progress_factory=progress_factory,
        hooks=(FeedbackOnlyHook(partial(render_pdf_feedback, title=title)),),
        read_pools=viewer.read_pools,
        write_pools=viewer.write_pools,
    )
