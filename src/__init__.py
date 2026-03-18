import re
import time
import random
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue, Empty
from urllib.parse import urlparse, unquote, urlencode
from urllib.request import Request, urlopen

from calibre import random_user_agent
from calibre.customize import InterfaceActionBase
from calibre.ebooks.metadata import check_isbn
from calibre.ebooks.metadata.book.base import Metadata
from calibre.ebooks.BeautifulSoup import BeautifulSoup
from calibre.gui2 import error_dialog, info_dialog
from calibre.gui2.actions import InterfaceAction
from calibre.utils.config import JSONConfig
from qt.core import QToolButton, QIcon, QPixmap
from bs4 import Tag

DOUBAN_BOOK_BASE = "https://book.douban.com/"
DOUBAN_SEARCH_JSON_URL = "https://www.douban.com/j/search"
DOUBAN_SEARCH_URL = "https://www.douban.com/search"
DOUBAN_BOOK_URL = 'https://book.douban.com/subject/%s/'
DOUBAN_BOOK_CAT = "1001"
DOUBAN_CONCURRENCY_SIZE = 5  # 并发查询数
DOUBAN_BOOK_URL_PATTERN = re.compile(".*/subject/(\\d+)/?")
PROVIDER_NAME = "NeoDouban"
PROVIDER_ID = "douban"
PROVIDER_VERSION = (1, 0, 0)
PROVIDER_AUTHOR = 'exhen'


class DoubanBookSearcher:

    def __init__(self, max_workers, douban_delay_enable, douban_login_cookie):
        self.book_parser = DoubanBookHtmlParser()
        self.max_workers = max_workers
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='douban_async')
        self.douban_delay_enable = douban_delay_enable
        self.douban_login_cookie = douban_login_cookie

    def calc_url(self, href):
        query = urlparse(href).query
        params = {item.split('=')[0]: item.split('=')[1] for item in query.split('&')}
        url = unquote(params['url'])
        if DOUBAN_BOOK_URL_PATTERN.match(url):
            return url

    def load_book_urls_new(self, query, log):
        params = {"cat": DOUBAN_BOOK_CAT, "q": query}
        url = DOUBAN_SEARCH_URL + "?" + urlencode(params)
        log.info(f'Load books by search url: {url}')
        res = urlopen(Request(url, headers=self.get_headers(), method='GET'))
        book_urls = []
        if res.status in [200, 201]:
            html_content = self.get_res_content(res)
            if self.is_prohibited(html_content, log):
                return book_urls
            html = BeautifulSoup(html_content)
            alist = html.select('a.nbg')
            for link in alist:
                href = link.get('href', '')
                parsed = self.calc_url(href)
                if parsed:
                    if len(book_urls) < self.max_workers:
                        book_urls.append(parsed)
        return book_urls

    def search_books(self, query, log):
        book_urls = self.load_book_urls_new(query, log)
        books = []
        futures = [self.thread_pool.submit(self.load_book, book_url, log) for book_url in book_urls]
        for future in as_completed(futures):
            book = future.result()
            if self.is_valid_book(book):
                books.append(book)
        return books

    def load_book(self, url, log):
        book = None
        start_time = time.time()
        if self.douban_delay_enable:
            self.random_sleep(log)
        res = urlopen(Request(url, headers=self.get_headers(), method='GET'))
        if res.status in [200, 201]:
            book_detail_content = self.get_res_content(res)
            if self.is_prohibited(book_detail_content, log):
                return
            log.info("Downloaded:{} Successful,Time {:.0f}ms".format(url, (time.time() - start_time) * 1000))
            try:
                book = self.book_parser.parse_book(url, book_detail_content)
                if not self.is_valid_book(book):
                    log.info(f"Parse book content error: {book_detail_content}")
            except Exception as e:
                log.info(f"Parse book content error: {e} \n Content: {book_detail_content}")
        return book

    def is_valid_book(self, book):
        return book is not None and book.get('title', None)

    def is_prohibited(self, html_content, log):
        prohibited = html_content is not None and '<title>禁止访问</title>' in html_content
        if prohibited:
            html = BeautifulSoup(html_content)
            html_content = html.select_one('div#content')
            log.info(f'Douban网页访问失败：{html_content}')
        return prohibited

    def get_res_content(self, res):
        encoding = res.info().get('Content-Encoding')
        if encoding == 'gzip':
            res_content = gzip.decompress(res.read())
        else:
            res_content = res.read()
        return res_content.decode(res.headers.get_content_charset())

    def get_headers(self):
        headers = {'User-Agent': random_user_agent(), 'Accept-Encoding': 'gzip, deflate'}
        if self.douban_login_cookie:
            headers['Cookie'] = self.douban_login_cookie
        return headers

    def random_sleep(self, log):
        random_sec = random.random() / 10
        log.info("Random sleep time {}s".format(random_sec))
        time.sleep(random_sec)


class DoubanBookHtmlParser:
    def __init__(self):
        self.id_pattern = DOUBAN_BOOK_URL_PATTERN
        self.tag_pattern = re.compile("criteria = '(.+)'")

    def parse_book(self, url, book_content):
        book = {}
        html = BeautifulSoup(book_content)
        if html is None or html.select is None:  # html判空处理
            return None
        title_element = html.select("span[property='v:itemreviewed']")
        book['title'] = self.get_text(title_element)
        share_element = html.select("a[data-url]")
        if len(share_element):
            url = share_element[0].get('data-url')
        book['url'] = url
        id_match = self.id_pattern.match(url)
        if id_match:
            book['id'] = id_match.group(1)
        img_element = html.select("a.nbg")
        if len(img_element):
            cover = img_element[0].get('href', '')
            if not cover or cover.endswith('update_image'):
                book['cover'] = ''
            else:
                book['cover'] = cover
        rating_element = html.select("strong[property='v:average']")
        book['rating'] = self.get_rating(rating_element)
        # 评分人数
        try:
            votes_element = html.select("span[property='v:votes']")
            votes_text = self.get_text(votes_element, '0')
            book['rating_count'] = int(votes_text or '0')
        except Exception:
            book['rating_count'] = 0
        elements = html.select("span.pl")
        book['authors'] = []
        book['translators'] = []
        book['publisher'] = ''
        for element in elements:
            text = self.get_text(element)
            parent_ele = element.find_parent()
            if text.startswith("作者"):
                book['authors'].extend([self.get_text(author_element) for author_element in
                                        filter(self.author_filter, parent_ele.select("a"))])
            elif text.startswith("译者"):
                book['translators'].extend([self.get_text(translator_element) for translator_element in
                                            filter(self.author_filter, parent_ele.select("a"))])
            elif text.startswith("出版社"):
                book['publisher'] = self.get_tail(element)
            elif text.startswith("副标题"):
                book['title'] = book['title'] + ':' + self.get_tail(element)
            elif text.startswith("出版年"):
                book['publishedDate'] = self.get_tail(element)
            elif text.startswith("ISBN"):
                book['isbn'] = self.get_tail(element)
            elif text.startswith("丛书"):
                book['series'] = self.get_text(element.find_next_sibling())
        summary_element = html.select("div#link-report div.intro")
        book['description'] = ''
        if len(summary_element):
            book['description'] = str(summary_element[-1])
        book['tags'] = self.get_tags(book_content)
        book['source'] = {
            "id": PROVIDER_ID,
            "description": PROVIDER_NAME,
            "link": DOUBAN_BOOK_BASE
        }
        book['language'] = self.get_book_language(book['title'])
        return book

    def get_book_language(self, title):
        pattern = r'^[a-zA-Z\-_]+$'
        if title and ('英文版' in title or bool(re.match(pattern, title))):
            return 'en_US'
        return 'zh_CN'

    def get_tags(self, book_content):
        tag_match = self.tag_pattern.findall(book_content)
        if len(tag_match):
            return [tag.replace('7:', '') for tag in
                    filter(lambda tag: tag and tag.startswith('7:'), tag_match[0].split('|'))]
        return []

    def get_rating(self, rating_element):
        return float(self.get_text(rating_element, '0')) 

    def author_filter(self, a_element):
        a_href = a_element.get('href', '')
        return '/author' in a_href or '/search' in a_href

    def get_text(self, element, default_str=''):
        text = default_str
        if isinstance(element, Tag):
            text = element.get_text(strip=True)
        elif len(element) and isinstance(element[0], Tag):
            text = element[0].get_text(strip=True)
        return text if text else default_str

    def get_tail(self, element, default_str=''):
        text = default_str
        if isinstance(element, Tag) and element.next_siblings:
            for next_sibling in element.next_siblings:
                if isinstance(next_sibling, str):
                    text += next_sibling.strip()
                elif isinstance(next_sibling, Tag):
                    if not text:
                        text = self.get_text(next_sibling, default_str)
                    break
        return text if text else default_str


class DoubanActionBase(InterfaceActionBase):
    """
    Calibre 识别的界面动作“壳”插件，负责配置和创建真正的 GUI 动作类。
    """
    name = 'NeoDouban'
    description = '从豆瓣更新选中书籍的元数据，并将评分写入自定义列'
    supported_platforms = ['windows', 'osx', 'linux']
    author = PROVIDER_AUTHOR
    version = PROVIDER_VERSION
    minimum_calibre_version = (5, 0, 0)

    def __init__(self, *args, **kwargs):
        InterfaceActionBase.__init__(self, *args, **kwargs)
        self._prefs = None
        self.gui = None  # 由 load_actual_plugin 注入

    # 让 Calibre 知道这个插件是可配置的
    def is_customizable(self):
        return True

    # JSONConfig，用于保存本插件的配置
    @property
    def prefs(self):
        if self._prefs is None:
            self._prefs = JSONConfig('plugins/douban_action')
            self._prefs.setdefault('douban_concurrency_size', DOUBAN_CONCURRENCY_SIZE)
            self._prefs.setdefault('add_translator_to_author', True)
            self._prefs.setdefault('douban_delay_enable', True)
            self._prefs.setdefault('douban_search_with_author', True)
            self._prefs.setdefault('douban_login_cookie', '')
            self._prefs.setdefault('rating_custom_column', '')
            self._prefs.setdefault('rating_count_custom_column', '')
            self._prefs.setdefault('translator_custom_column', '')
            self._prefs.setdefault('replace_cover_by_default', True)
        return self._prefs

    # InterfaceActionBase 规范：返回真正的 GUI 动作实例
    def load_actual_plugin(self, gui):
        self.gui = gui
        ac = DoubanAction(gui, self.site_customization)
        ac.interface_action_base_plugin = self
        return ac

    # --- 配置界面（在“插件 → 配置”里打开） ---
    def config_widget(self):
        from qt.core import QWidget, QVBoxLayout, QFormLayout, QSpinBox, QCheckBox, QLineEdit, QComboBox, QLabel

        cw = QWidget()
        cw.l = QVBoxLayout(cw)
        form = QFormLayout()
        cw.l.addLayout(form)

        # 并发数
        spin_conc = QSpinBox(cw)
        spin_conc.setRange(1, 20)
        spin_conc.setValue(int(self.prefs.get('douban_concurrency_size')))
        form.addRow(QLabel('并发抓取数量'), spin_conc)

        # 随机延迟
        cb_delay = QCheckBox('启用随机延迟', cw)
        cb_delay.setChecked(bool(self.prefs.get('douban_delay_enable')))
        form.addRow(cb_delay)

        # 检索时附带作者
        cb_with_author = QCheckBox('检索时附带作者', cw)
        cb_with_author.setChecked(bool(self.prefs.get('douban_search_with_author')))
        form.addRow(cb_with_author)

        # 把译者写入作者
        cb_add_trans = QCheckBox('把译者也写入作者', cw)
        cb_add_trans.setChecked(bool(self.prefs.get('add_translator_to_author')))
        form.addRow(cb_add_trans)

        # 登录 Cookie
        le_cookie = QLineEdit(cw)
        le_cookie.setText(self.prefs.get('douban_login_cookie') or '')
        form.addRow(QLabel('豆瓣登录 Cookie'), le_cookie)

        # 评分自定义列（下拉框，只允许 float 类型列）
        combo_col = QComboBox(cw)
        combo_col.addItem(_('必须选择一个 float 类型自定义列'), '')

        # 评分人数自定义列（下拉框，只允许 int 类型列）
        combo_col_count = QComboBox(cw)
        combo_col_count.addItem(_('可选：选择一个 int 类型自定义列'), '')

        # 译者自定义列（下拉框，只允许 text 类型列）
        combo_col_translator = QComboBox(cw)
        combo_col_translator.addItem(_('可选：选择一个 text 类型自定义列'), '')

        try:
            gui = self.gui
            db = getattr(gui, 'current_db', None)
            field_metadata = getattr(db, 'field_metadata', None) if db is not None else None
            if field_metadata is not None:
                for key in field_metadata.custom_field_keys(include_composites=False):
                    try:
                        meta = field_metadata[key]
                    except Exception:
                        continue
                    if not meta.get('is_editable', True):
                        continue
                    datatype = meta.get('datatype', None)
                    label = meta.get('name') or key
                    # 评分列：只允许 float
                    if datatype == 'float':
                        combo_col.addItem(f'{label} ({key})', f'{key}::float')
                    # 评分人数列：只允许 int
                    if datatype == 'int':
                        combo_col_count.addItem(f'{label} ({key})', f'{key}::int')
                    # 译者列：只允许 text
                    if datatype == 'text':
                        combo_col_translator.addItem(f'{label} ({key})', f'{key}::text')

            current = self.prefs.get('rating_custom_column') or ''
            idx = combo_col.findData(current)
            if idx >= 0:
                combo_col.setCurrentIndex(idx)

            current_count = self.prefs.get('rating_count_custom_column') or ''
            idx2 = combo_col_count.findData(current_count)
            if idx2 >= 0:
                combo_col_count.setCurrentIndex(idx2)
            current_trans = self.prefs.get('translator_custom_column') or ''
            idx3 = combo_col_translator.findData(current_trans)
            if idx3 >= 0:
                combo_col_translator.setCurrentIndex(idx3)
        except Exception:
            pass

        form.addRow(QLabel('用于写入评分的自定义列（float 类型）'), combo_col)
        form.addRow(QLabel('用于写入评分人数的自定义列（int 类型，可选）'), combo_col_count)
        form.addRow(QLabel('用于写入译者信息的自定义列（text 类型，可选）'), combo_col_translator)

        # 把控件挂到 widget 上，供 save_settings 读取
        cw.spin_conc = spin_conc
        cw.cb_delay = cb_delay
        cw.cb_with_author = cb_with_author
        cw.cb_add_trans = cb_add_trans
        cw.le_cookie = le_cookie
        cw.combo_col = combo_col
        cw.combo_col_count = combo_col_count
        cw.combo_col_translator = combo_col_translator



        # 默认填充字段多选（使用一组复选框，按网格排列避免界面过长）
        default_fields = set(self.prefs.get('default_fill_fields') or [])
        cw.cb_replace_cover_default = QCheckBox('封面', cw)
        cw.cb_replace_cover_default.setChecked(bool(self.prefs.get('replace_cover_by_default', True)))
        cw.cb_fill_title = QCheckBox('标题', cw)
        cw.cb_fill_title.setChecked(not default_fields or 'title' in default_fields)
        cw.cb_fill_authors = QCheckBox('作者', cw)
        cw.cb_fill_authors.setChecked(not default_fields or 'authors' in default_fields)
        cw.cb_fill_comments = QCheckBox('简介', cw)
        cw.cb_fill_comments.setChecked(not default_fields or 'comments' in default_fields)
        cw.cb_fill_series = QCheckBox('系列', cw)
        cw.cb_fill_series.setChecked(not default_fields or 'series' in default_fields)
        cw.cb_fill_tags = QCheckBox('标签', cw)
        cw.cb_fill_tags.setChecked(not default_fields or 'tags' in default_fields)
        cw.cb_fill_publisher = QCheckBox('出版社', cw)
        cw.cb_fill_publisher.setChecked(not default_fields or 'publisher' in default_fields)
        cw.cb_fill_rating = QCheckBox('评分(Calibre星级)', cw)
        cw.cb_fill_rating.setChecked(not default_fields or 'rating' in default_fields)
        cw.cb_fill_custom_rating = QCheckBox('评分(自定义列)', cw)
        cw.cb_fill_custom_rating.setChecked(not default_fields or 'custom_rating' in default_fields)
        cw.cb_fill_custom_rating_count = QCheckBox('评分人数(自定义列)', cw)
        cw.cb_fill_custom_rating_count.setChecked(not default_fields or 'custom_rating_count' in default_fields)
        cw.cb_fill_translator = QCheckBox('译者(自定义列)', cw)
        cw.cb_fill_translator.setChecked(not default_fields or 'custom_translator' in default_fields)
        

        from qt.core import QGridLayout
        grid = QGridLayout()
        grid.addWidget(QLabel('默认填充的字段（无检查模式下也仅填充这些）：'), 0, 0, 1, 3)
        grid.addWidget(cw.cb_replace_cover_default, 1, 0)
        grid.addWidget(cw.cb_fill_title, 2, 0)
        grid.addWidget(cw.cb_fill_authors, 3, 0)
        grid.addWidget(cw.cb_fill_comments, 1, 1)
        grid.addWidget(cw.cb_fill_series, 2, 1)
        grid.addWidget(cw.cb_fill_tags, 3, 1)
        grid.addWidget(cw.cb_fill_publisher, 1, 2)
        grid.addWidget(cw.cb_fill_rating, 2, 2)
        grid.addWidget(cw.cb_fill_custom_rating, 3, 2)
        grid.addWidget(cw.cb_fill_custom_rating_count, 1, 3)
        grid.addWidget(cw.cb_fill_translator, 2, 3)
        cw.l.addLayout(grid)

        # 给设置对话框一个合适的默认大小
        cw.resize(520, 360)

        return cw

    def save_settings(self, config_widget):
        # 保存配置到 JSONConfig
        self.prefs['douban_concurrency_size'] = int(config_widget.spin_conc.value())
        self.prefs['douban_delay_enable'] = bool(config_widget.cb_delay.isChecked())
        self.prefs['douban_search_with_author'] = bool(config_widget.cb_with_author.isChecked())
        self.prefs['add_translator_to_author'] = bool(config_widget.cb_add_trans.isChecked())
        self.prefs['douban_login_cookie'] = config_widget.le_cookie.text().strip()

        data = config_widget.combo_col.currentData() or ''
        self.prefs['rating_custom_column'] = data
        data_count = config_widget.combo_col_count.currentData() or ''
        self.prefs['rating_count_custom_column'] = data_count
        data_trans = config_widget.combo_col_translator.currentData() or ''
        self.prefs['translator_custom_column'] = data_trans
        self.prefs['replace_cover_by_default'] = bool(config_widget.cb_replace_cover_default.isChecked())

        # 默认填充字段
        fields = []
        if config_widget.cb_fill_title.isChecked():
            fields.append('title')
        if config_widget.cb_fill_authors.isChecked():
            fields.append('authors')
        if config_widget.cb_fill_series.isChecked():
            fields.append('series')
        if config_widget.cb_fill_tags.isChecked():
            fields.append('tags')
        if config_widget.cb_fill_publisher.isChecked():
            fields.append('publisher')
        if config_widget.cb_fill_rating.isChecked():
            fields.append('rating')
        if config_widget.cb_fill_custom_rating.isChecked():
            fields.append('custom_rating')
        if config_widget.cb_fill_custom_rating_count.isChecked():
            fields.append('custom_rating_count')
        if config_widget.cb_fill_translator.isChecked():
            fields.append('custom_translator')
        if config_widget.cb_fill_comments.isChecked():
            fields.append('comments')
        self.prefs['default_fill_fields'] = fields


class DoubanAction(InterfaceAction):
    """
    真正出现在工具栏中的 GUI 动作类。
    """
    name = 'New Douban Books'

    #: 工具栏按钮和菜单项说明 (文本, 图标文件名, 提示文本, 快捷键)
    #: 图标名使用 calibre 内置图标（位于 resources/images），避免额外资源文件
    action_spec = (_('Douban metadata'), None,
                   _('从豆瓣更新元数据（含评分自定义列）'), None)
    action_add_menu = True
    # 工具栏按钮点击时直接弹出菜单
    popup_type = QToolButton.ToolButtonPopupMode.InstantPopup

    def __init__(self, gui, site_customization):
        InterfaceAction.__init__(self, gui, site_customization)
        self.book_searcher = None
        self._rating_custom_column_key = ''
        self._rating_custom_column_datatype = ''

    @property
    def prefs(self):
        # 复用 Base 插件的配置
        return self.interface_action_base_plugin.prefs

    def genesis(self):
        # 在 GUI 创建好后调用，这里可以拿到 self.gui
        self.qaction.setIconText(_('Douban'))

        # 加载自定义 SVG 图标作为插件 logo
        try:
            res = self.load_resources(['logo.svg'])
            data = res.get('logo.svg')
            if data:
                pix = QPixmap()
                if pix.loadFromData(data):
                    icon = QIcon(pix)
                    self.qaction.setIcon(icon)
        except Exception:
            # 图标加载失败时保持无图标，不影响功能
            pass

        # 构建下拉菜单：带检查 / 不检查 / 设置
        menu = self.qaction.menu()
        self.create_menu_action(
            menu, 'douban_fill_checked',
            _('填充元数据（带检查）'),
            triggered=self.run_fill_checked
        )
        self.create_menu_action(
            menu, 'douban_fill_unchecked',
            _('填充元数据（不检查）'),
            triggered=self.run_fill_unchecked
        )
        # 在豆瓣中打开
        self.create_menu_action(
            menu, 'douban_open_in_browser',
            _('在豆瓣中打开'),
            triggered=self.run_open_in_douban
        )
        menu.addSeparator()
        self.create_menu_action(
            menu, 'douban_settings',
            _('设置'),
            triggered=self.run_settings
        )

        # 初始化抓取器
        concurrency_size = int(self.prefs.get('douban_concurrency_size', DOUBAN_CONCURRENCY_SIZE))
        douban_delay_enable = bool(self.prefs.get('douban_delay_enable', True))
        douban_login_cookie = self.prefs.get('douban_login_cookie', None)
        self.douban_search_with_author = bool(self.prefs.get('douban_search_with_author', True))
        self.book_searcher = DoubanBookSearcher(concurrency_size, douban_delay_enable, douban_login_cookie)

        # 解析评分目标列配置
        rating_col = self.prefs.get('rating_custom_column') or ''
        self._rating_custom_column_key, self._rating_custom_column_datatype = self._parse_rating_custom_column(
            rating_col
        )
        rating_count_col = self.prefs.get('rating_count_custom_column') or ''
        self._rating_count_column_key, self._rating_count_column_datatype = self._parse_rating_custom_column(
            rating_count_col
        )
        translator_col = self.prefs.get('translator_custom_column') or ''
        self._translator_column_key, self._translator_column_datatype = self._parse_rating_custom_column(
            translator_col
        )

    # --- 菜单入口 ---
    def run_fill_checked(self):
        self._run_update(with_check=True)

    def run_fill_unchecked(self):
        self._run_update(with_check=False)

    def run_open_in_douban(self):
        """
        使用系统浏览器打开当前选中书籍对应的豆瓣页面：
        https://book.douban.com/subject/${douban_id}
        """
        import webbrowser

        gui = self.gui
        db = gui.current_db
        ids = list(gui.library_view.get_selected_ids())
        if not ids:
            info_dialog(gui, _('Douban metadata'),
                        _('请先在书库中选择至少一本书。'), show=True)
            return

        # 仅使用第一本选中书
        book_id = ids[0]
        mi = db.new_api.get_metadata(book_id)
        identifiers = mi.identifiers or {}
        douban_id = identifiers.get(PROVIDER_ID)
        if not douban_id:
            error_dialog(gui, _('Douban metadata'),
                         _('当前书籍没有保存豆瓣 ID（标识 douban）。'),
                         show=True)
            return

        url = f'https://book.douban.com/subject/{douban_id}'
        try:
            webbrowser.open(url)
        except Exception as e:
            error_dialog(gui, _('Douban metadata'),
                         f'无法在浏览器中打开豆瓣链接：{e}',
                         show=True)

    def run_settings(self):
        # 打开本插件自己的配置对话框（不要跳到全局 Calibre 设置）
        try:
            self.interface_action_base_plugin.do_user_config(self.gui)
        except Exception:
            error_dialog(
                self.gui,
                _('Douban metadata'),
                _('无法打开插件设置，请在“首选项 → 插件”中手动配置。'),
                show=True,
            )

    # --- 主动作 ---
    def _run_update(self, with_check):
        gui = self.gui
        db = gui.current_db
        ids = list(gui.library_view.get_selected_ids())
        if not ids:
            info_dialog(gui, _('Douban metadata'),
                        _('请先在书库中选择至少一本书。'), show=True)
            return

        col_key = self._rating_custom_column_key
        col_dtype = self._rating_custom_column_datatype
        if not col_key:
            error_dialog(gui, _('Douban metadata'),
                         _('尚未在插件设置中选择用于保存评分的自定义列。'),
                         show=True)
            return

        col_count_key = getattr(self, '_rating_count_column_key', '')
        col_count_dtype = getattr(self, '_rating_count_column_datatype', '')
        col_trans_key = getattr(self, '_translator_column_key', '')
        col_trans_dtype = getattr(self, '_translator_column_datatype', '')

        add_translator_to_author = bool(self.prefs.get('add_translator_to_author', True))

        new_api = getattr(db, 'new_api', None)
        if new_api is None:
            error_dialog(gui, _('Douban metadata'),
                         _('当前 Calibre 版本不支持 new_api 接口，无法写入自定义列。'),
                         show=True)
            return

        for book_id in ids:
            try:
                self._update_one_book(new_api, book_id,
                                      col_key, col_dtype,
                                      col_count_key, col_count_dtype,
                                      col_trans_key, col_trans_dtype,
                                      add_translator_to_author, with_check)
            except Exception as e:
                # 老版本 GUI 没有 print_error，直接弹错误对话框并继续处理后续图书
                error_dialog(gui, _('Douban metadata'),
                             f'Douban update failed for book {book_id}: {e}',
                             show=True)

        info_dialog(gui, _('Douban metadata'),
                    _('豆瓣元数据更新完成。'), show=True)

    def _update_one_book(self, db_api, book_id,
                         col_key, col_dtype,
                         col_count_key, col_count_dtype,
                         col_trans_key, col_trans_dtype,
                         add_translator_to_author, with_check):
        """
        对单本书执行：调整搜索参数 -> 豆瓣搜索 -> 可选检查 -> 写入。
        支持在检查窗口中“返回修改搜索”。
        """
        mi = db_api.get_metadata(book_id)
        title = mi.title
        authors = mi.authors or []
        identifiers = mi.identifiers or {}
        isbn = check_isbn(identifiers.get('isbn', None))

        # 默认填充字段（可在设置中配置），检查对话框中可按字段再调整
        default_fields = set(self.prefs.get('default_fill_fields') or [])
        all_ids = {
            'title', 'authors', 'series', 'tags', 'publisher',
            'rating', 'comments',
            'custom_rating', 'custom_rating_count', 'custom_translator'
        }
        if not default_fields:
            default_fields = all_ids

        self._apply_rating_custom = 'custom_rating' in default_fields
        self._apply_rating_count_custom = 'custom_rating_count' in default_fields
        self._apply_translator_custom = 'custom_translator' in default_fields

        while True:
            # 1. 让用户调整用于搜索的字段：优先使用 ISBN，其次“书名+作者”，最后仅书名
            if isbn:
                base_kw = isbn
            elif self.douban_search_with_author and title and authors:
                base_kw = f'{title} {" ".join(authors)}'
            else:
                base_kw = title
            search_keyword = self._ask_search_params(title, authors, isbn, base_kw)
            if not search_keyword:
                return  # 用户取消或关闭搜索参数对话框

            # 2. 执行豆瓣搜索
            log = self  # 使用自身作为日志对象，提供 info/error/print 接口
            books = self.book_searcher.search_books(search_keyword, log)
            if not books and title and search_keyword != title:
                books = self.book_searcher.search_books(title, log)
            if not books:
                return

            book = books[0]
            new_mi = self.to_metadata(book, add_translator_to_author, log)

            # 3. 可选：弹出检查对话框，显示旧值与新值
            if with_check:
                action = self._confirm_changes(
                    mi, new_mi, db_api, book_id,
                    col_key, col_dtype,
                    col_count_key, col_count_dtype,
                    col_trans_key, col_trans_dtype,
                )
                if action == 'skip':
                    return
                if action == 'rescan':
                    # 回到 while 顶部，重新调整搜索参数并搜索
                    continue
            else:
                # 无检查模式下：仅按默认填充字段覆盖 new_mi
                selected = default_fields
                if 'title' not in selected:
                    new_mi.title = mi.title
                if 'authors' not in selected:
                    new_mi.authors = list(mi.authors or [])
                if 'series' not in selected:
                    new_mi.series = getattr(mi, 'series', None)
                if 'tags' not in selected:
                    new_mi.tags = list(mi.tags or [])
                if 'publisher' not in selected:
                    new_mi.publisher = mi.publisher
                if 'rating' not in selected:
                    new_mi.rating = getattr(mi, 'rating', None)
                if 'comments' not in selected:
                    new_mi.comments = getattr(mi, 'comments', None)

            # 4. 写回标准字段
            db_api.set_metadata(book_id, new_mi)

            # 写入自定义列评分
            rating_val = book.get('rating', None)
            val = self._format_rating_for_datatype(rating_val, col_dtype)
            if val is not None and getattr(self, '_apply_rating_custom', True):
                db_api.set_field(col_key, {book_id: val})

            # 写入评分人数（如果已配置）
            if col_count_key:
                count_val = book.get('rating_count', None)
                v2 = self._format_rating_for_datatype(count_val, col_count_dtype or 'int')
                if v2 is not None and getattr(self, '_apply_rating_count_custom', True):
                    db_api.set_field(col_count_key, {book_id: v2})

            # 写入译者信息（如果已配置）
            if col_trans_key and getattr(self, '_apply_translator_custom', True):
                translators = book.get('translators', [])
                trans_text = ', '.join(translators) if translators else ''
                if trans_text:
                    db_api.set_field(col_trans_key, {book_id: trans_text})

            # 写入封面（如果勾选替换封面）
            if getattr(self, '_replace_cover', False):
                new_cover_url = getattr(new_mi, 'cover', None)
                if new_cover_url:
                    try:
                        if hasattr(self, 'book_searcher') and hasattr(self.book_searcher, 'get_headers'):
                            headers = self.book_searcher.get_headers()
                        else:
                            headers = {'User-Agent': random_user_agent(), 'Accept-Encoding': 'gzip, deflate'}
                        headers.setdefault('Referer', DOUBAN_BOOK_BASE)
                        data = urlopen(Request(new_cover_url, headers=headers, method='GET')).read()
                        if data:
                            db_api.set_cover({book_id: data})
                    except Exception:
                        pass

            return  # 正常完成后退出 while 循环

    def _ask_search_params(self, title, authors, isbn, default_keyword):
        """
        搜索前弹出对话框，让用户调整用于搜索的字段，并返回最终搜索关键字。
        返回 None 表示用户取消。
        """
        from qt.core import QDialog, QVBoxLayout, QFormLayout, QLineEdit, QDialogButtonBox, QLabel

        dlg = QDialog(self.gui)
        dlg.setWindowTitle(_('调整豆瓣搜索参数'))
        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel('你可以在搜索前调整搜索关键字：'))

        form = QFormLayout()
        le_kw = QLineEdit(dlg)
        le_kw.setText(default_keyword or (title or ''))
        # 让输入框宽度大致可容纳 30 个汉字
        le_kw.setMinimumWidth(30 * le_kw.fontMetrics().averageCharWidth())

        form.addRow(_('搜索关键字'), le_kw)
        layout.addLayout(form)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            dlg
        )
        layout.addWidget(btns)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)

        dlg.resize(520, 260)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return None

        return le_kw.text().strip() or None

    # --- 复用原来的元数据转换 ---
    def to_metadata(self, book, add_translator_to_author, log):
        if book:
            authors = (book['authors'] + book['translators']
                       ) if add_translator_to_author else book['authors']
            mi = Metadata(book['title'], authors)
            mi.identifiers = {PROVIDER_ID: book['id']}
            mi.url = book['url']
            mi.cover = book.get('cover', None)
            mi.publisher = book['publisher']
            pubdate = book.get('publishedDate', None)
            if pubdate:
                try:
                    if re.compile('^\\d{4}-\\d+$').match(pubdate):
                        mi.pubdate = datetime.strptime(pubdate, '%Y-%m')
                    elif re.compile('^\\d{4}-\\d+-\\d+$').match(pubdate):
                        mi.pubdate = datetime.strptime(pubdate, '%Y-%m-%d')
                except:
                    log.error('Failed to parse pubdate %r' % pubdate)
            mi.comments = book['description']
            mi.tags = book.get('tags', [])
            mi.rating = book['rating']
            # 保存译者列表供检查视图与自定义列使用
            mi._douban_translators = book.get('translators', [])
            # 评分人数（用于检查视图展示）
            mi.rating_count = book.get('rating_count', None)
            isbn = book.get('isbn', '') or ''
            if isbn:
                try:
                    mi.identifiers['isbn'] = isbn
                except Exception:
                    pass
            mi.series = book.get('series', [])
            mi.language = book.get('language', 'zh_CN')
            try:
                log.print(f'parsed book {book}')
            except Exception:
                pass
            return mi

    # --- 工具方法 ---
    def _parse_rating_custom_column(self, raw):
        raw = (raw or '').strip()
        if not raw:
            return '', ''
        if '::' in raw:
            key, datatype = raw.split('::', 1)
            key = (key or '').strip()
            datatype = (datatype or '').strip()
            if key.startswith('#') and datatype:
                return key, datatype
            return '', ''
        if raw.startswith('#'):
            return raw, ''
        return '', ''

    def _format_rating_for_datatype(self, rating, datatype):
        if rating is None:
            return None
        try:
            r = float(rating)
        except Exception:
            return None
        if datatype == 'int':
            return int(round(r))
        if datatype in {'rating', 'float'}:
            return r
        if datatype == 'text' or not datatype:
            return f'{r:.2f}'.rstrip('0').rstrip('.')
        return None

    # --- 检查视图，对比旧值与新值 ---
    def _confirm_changes(self, old_mi, new_mi, db_api, book_id,
                         col_key, col_dtype,
                         col_count_key, col_count_dtype,
                         col_trans_key, col_trans_dtype):
        from qt.core import (
            QDialog, QVBoxLayout, QTableWidget, QTableWidgetItem,
            QPushButton, QHBoxLayout, QLabel, QHeaderView, Qt, QColor, QCheckBox, QPixmap
        )

        def _to_str(v):
            if isinstance(v, list):
                return ', '.join(map(str, v))
            return '' if v is None else str(v)

        # 当前自定义列旧值
        try:
            old_custom = db_api.field_for(col_key, book_id, default=None)
        except Exception:
            old_custom = None

        # 当前评分人数列旧值
        old_count_custom = None
        if col_count_key:
            try:
                old_count_custom = db_api.field_for(col_count_key, book_id, default=None)
            except Exception:
                old_count_custom = None

        # 当前译者列旧值
        old_trans_custom = None
        if col_trans_key:
            try:
                old_trans_custom = db_api.field_for(col_trans_key, book_id, default=None)
            except Exception:
                old_trans_custom = None

        # 计算新评分值
        new_rating_val = getattr(new_mi, 'rating', None)
        new_custom_val = self._format_rating_for_datatype(new_rating_val, col_dtype)
        rating_count = getattr(new_mi, 'rating_count', None)
        new_count_custom_val = self._format_rating_for_datatype(rating_count, col_count_dtype or 'int')
        translators_list = getattr(new_mi, '_douban_translators', []) or []
        new_trans_custom_val = ', '.join(translators_list) if translators_list else ''

        # 行定义：[(逻辑字段ID, 显示名称, 旧值, 新值), ...]
        rows = [
            ('title', '标题', _to_str(old_mi.title), _to_str(new_mi.title)),
            ('authors', '作者', _to_str(old_mi.authors), _to_str(new_mi.authors)),
            ('series', '系列', _to_str(getattr(old_mi, 'series', None)), _to_str(getattr(new_mi, 'series', None))),
            ('tags', '标签', _to_str(old_mi.tags), _to_str(new_mi.tags)),
            ('publisher', '出版社', _to_str(old_mi.publisher), _to_str(new_mi.publisher)),
            ('rating', '评分', _to_str(getattr(old_mi, 'rating', None)), _to_str(new_rating_val)),
            ('comments', '简介', _to_str(getattr(old_mi, 'comments', None)), _to_str(getattr(new_mi, 'comments', None))),
            ('custom_rating', f'自定义列 {col_key}', _to_str(old_custom), _to_str(new_custom_val)),
        ]

        if col_count_key:
            rows.append(
                ('custom_rating_count',
                 f'评分人数列 {col_count_key}',
                 _to_str(old_count_custom),
                 _to_str(new_count_custom_val))
            )

        if col_trans_key:
            rows.append(
                ('custom_translator',
                 f'译者列 {col_trans_key}',
                 _to_str(old_trans_custom),
                 _to_str(new_trans_custom_val))
            )

        dlg = QDialog(self.gui)
        dlg.setWindowTitle(_('检查即将写入的豆瓣元数据'))
        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel('请检查以下字段的旧值与新值：'))

        table = QTableWidget(len(rows), 3, dlg)
        table.setHorizontalHeaderLabels(['字段', '当前值', '新值'])
        default_fields = set(self.prefs.get('default_fill_fields') or [])
        for i, (fid, label, old_val, new_val) in enumerate(rows):
            item_field = QTableWidgetItem(label)
            # 可勾选：控制该字段是否应用
            item_field.setFlags(item_field.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            if not default_fields or fid in default_fields:
                item_field.setCheckState(Qt.CheckState.Checked)
            else:
                item_field.setCheckState(Qt.CheckState.Unchecked)
            table.setItem(i, 0, item_field)
            table.setItem(i, 1, QTableWidgetItem(old_val))
            table.setItem(i, 2, QTableWidgetItem(new_val))

        # 根据勾选状态设置行底色：勾选为浅绿色，未勾选无底色
        def update_row_colors():
            # 根据当前配色方案自动选择适合的浅绿色，兼容深色/浅色模式
            pal = table.palette()
            base_color = pal.base().color()
            # lightness < 128 视为深色模式
            is_dark = base_color.lightness() < 128
            if is_dark:
                # 深色模式：在当前背景色基础上稍微提亮并偏向绿色
                checked_color = QColor(base_color)
                checked_color.setGreen(min(255, checked_color.green() + 80))
                checked_color.setRed(max(0, checked_color.red() - 20))
                checked_color.setBlue(max(0, checked_color.blue() - 20))
            else:
                # 浅色模式：使用柔和浅绿色
                checked_color = QColor(200, 255, 200)
            for row in range(table.rowCount()):
                item0 = table.item(row, 0)
                if item0 is None:
                    continue
                if item0.checkState() == Qt.CheckState.Checked:
                    for col in range(table.columnCount()):
                        it = table.item(row, col)
                        if it:
                            it.setBackground(checked_color)
                else:
                    for col in range(table.columnCount()):
                        it = table.item(row, col)
                        if it:
                            it.setBackground(QColor())  # 默认背景

        update_row_colors()
        table.itemChanged.connect(lambda _item: update_row_colors())
        # 为每列设置一个合适的默认宽度，避免过宽/过窄
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        table.setColumnWidth(0, 110)
        table.setColumnWidth(1, 240)
        table.setColumnWidth(2, 240)
        layout.addWidget(table)

        # 封面缩略图预览（当前封面 vs 豆瓣封面）和“是否替换封面”选项
        thumbs = QHBoxLayout()
        thumb_width, thumb_height = 96, 128

        # 当前封面区域（图片 + 尺寸）
        old_box = QVBoxLayout()
        lbl_old_cover = QLabel(dlg)
        lbl_old_cover.setFixedSize(thumb_width, thumb_height)
        lbl_old_cover.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_old_size = QLabel(_('无'), dlg)
        lbl_old_size.setAlignment(Qt.AlignmentFlag.AlignCenter)
        old_box.addWidget(lbl_old_cover)
        old_box.addWidget(lbl_old_size)

        # 豆瓣封面区域（图片 + 尺寸）
        new_box = QVBoxLayout()
        lbl_new_cover = QLabel(dlg)
        lbl_new_cover.setFixedSize(thumb_width, thumb_height)
        lbl_new_cover.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_new_size = QLabel(_('无'), dlg)
        lbl_new_size.setAlignment(Qt.AlignmentFlag.AlignCenter)
        new_box.addWidget(lbl_new_cover)
        new_box.addWidget(lbl_new_size)

        thumbs.addLayout(old_box)
        thumbs.addLayout(new_box)
        layout.addLayout(thumbs)

        # 是否替换封面单选（复选框）
        cb_replace_cover = QCheckBox(_('替换封面'), dlg)
        cb_replace_cover.setChecked(bool(self.prefs.get('replace_cover_by_default', True)))
        layout.addWidget(cb_replace_cover)

        # 预览新封面（豆瓣）
        new_cover_url = getattr(new_mi, 'cover', None)
        if new_cover_url:
            try:
                # 使用与搜索器相同的请求头，并设置 Referer，避免豆瓣拒绝直链
                if hasattr(self, 'book_searcher') and hasattr(self.book_searcher, 'get_headers'):
                    headers = self.book_searcher.get_headers()
                else:
                    headers = {'User-Agent': random_user_agent(), 'Accept-Encoding': 'gzip, deflate'}
                # 部分站点需要 Referer 才返回真实封面
                headers.setdefault('Referer', DOUBAN_BOOK_BASE)
                data = urlopen(Request(new_cover_url, headers=headers, method='GET')).read()
                pix = QPixmap()
                if pix.loadFromData(data):
                    # 显示缩略图
                    lbl_new_cover.setPixmap(pix.scaled(
                        thumb_width, thumb_height,
                        Qt.AspectRatioMode.KeepAspectRatio,
                        Qt.TransformationMode.SmoothTransformation
                    ))
                    # 显示原始尺寸
                    lbl_new_size.setText(f'{pix.width()} x {pix.height()}')
            except Exception:
                pass

        # 预览旧封面（当前库中的封面）
        try:
            db_api = db_api  # for clarity; db_api 是 Cache.new_api
            old_pix = db_api.cover(book_id, as_pixmap=True)
            if old_pix:
                # 显示缩略图
                lbl_old_cover.setPixmap(old_pix.scaled(
                    thumb_width, thumb_height,
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation
                ))
                # 显示原始尺寸
                lbl_old_size.setText(f'{old_pix.width()} x {old_pix.height()}')
        except Exception:
            # 回退为文字提示
            try:
                has_old_cover = not old_mi.is_null('cover')
            except Exception:
                has_old_cover = False
            lbl_old_cover.setText('当前已有封面' if has_old_cover else '当前无封面')

        # 设置一个合适的默认对话框尺寸，并允许根据内容伸缩
        dlg.resize(800, 800)

        btn_box = QHBoxLayout()
        btn_ok = QPushButton('应用此更改', dlg)
        btn_skip = QPushButton('跳过此书', dlg)
        btn_rescan = QPushButton('返回修改搜索', dlg)
        btn_box.addWidget(btn_ok)
        btn_box.addWidget(btn_skip)
        btn_box.addWidget(btn_rescan)
        layout.addLayout(btn_box)

        result = {'value': 'skip'}

        def accept():
            result['value'] = 'apply'
            dlg.accept()

        def skip():
            result['value'] = 'skip'
            dlg.reject()

        def rescan():
            result['value'] = 'rescan'
            dlg.reject()

        btn_ok.clicked.connect(accept)
        btn_skip.clicked.connect(skip)
        btn_rescan.clicked.connect(rescan)

        dlg.exec()

        # 根据勾选结果调整 new_mi / 写入自定义列标志
        if result['value'] == 'apply':
            selected = set()
            for i, (fid, _label, _old, _new) in enumerate(rows):
                item = table.item(i, 0)
                if item is not None and item.checkState() == Qt.CheckState.Checked:
                    selected.add(fid)

            # 内置字段：未勾选则保留旧值
            if 'title' not in selected:
                new_mi.title = old_mi.title
            if 'authors' not in selected:
                new_mi.authors = list(old_mi.authors or [])
            if 'series' not in selected:
                new_mi.series = getattr(old_mi, 'series', None)
            if 'tags' not in selected:
                new_mi.tags = list(old_mi.tags or [])
            if 'publisher' not in selected:
                new_mi.publisher = old_mi.publisher
            if 'rating' not in selected:
                new_mi.rating = getattr(old_mi, 'rating', None)
            if 'comments' not in selected:
                new_mi.comments = getattr(old_mi, 'comments', None)

            # 自定义评分列
            self._apply_rating_custom = 'custom_rating' in selected
            # 自定义评分人数列
            self._apply_rating_count_custom = 'custom_rating_count' in selected
            # 自定义译者列
            self._apply_translator_custom = 'custom_translator' in selected

            # 封面是否替换由“替换封面”复选框控制
            self._replace_cover = cb_replace_cover.isChecked()
        else:
            self._replace_cover = False

        return result['value']

    # --- 日志兼容方法，供 DoubanBookSearcher / to_metadata 调用 ---
    def info(self, msg, *args, **kwargs):
        try:
            text = str(msg)
            self.gui.status_bar.showMessage(text, 5000)
        except Exception:
            pass

    def error(self, msg, *args, **kwargs):
        try:
            error_dialog(self.gui, _('Douban metadata'), str(msg), show=True)
        except Exception:
            pass

    def print(self, msg, *args, **kwargs):
        # 调试信息，同样走状态栏
        self.info(msg)
